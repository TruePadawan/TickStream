"""Spawn isolated client readers when one Python process cannot keep up."""
import asyncio
from dataclasses import fields
import multiprocessing
import os
import signal


async def serve_clients(clients, args, start_ns, end_ns):
    """Use the same reader and measurement window in every worker."""
    from metrics import clock_ns
    from run import read_client
    abort = asyncio.Event()
    tasks = [asyncio.create_task(read_client(c, args, start_ns, end_ns, abort)) for c in clients]
    try:
        await asyncio.wait_for(abort.wait(), max(0, (end_ns - clock_ns()) / 1e9))
    except asyncio.TimeoutError:
        pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def worker_entry(clients, args, pipe):
    """Own transferred sockets and send bounded histogram/state records back to the controller."""
    error = None
    try:
        # Socket reconstruction in a spawned process resets Python's blocking mode.
        for client in clients:
            client.sock.setblocking(False)
        pipe.send({"kind": "ready"})
        window = pipe.recv()
        if window is None:
            return
        asyncio.run(serve_clients(clients, args, *window))
    except KeyboardInterrupt:
        error = "Worker interrupted during an incomplete run"
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        payload = [{field.name: getattr(client, field.name) for field in fields(client) if field.name != "sock"}
                   for client in clients]
        try:
            pipe.send({"kind": "result", "clients": payload, "error": error})
        except (OSError, EOFError, BrokenPipeError):
            pass
        for client in clients:
            client.sock.close()
        pipe.close()


class WorkerPool:
    """Coordinates readiness and result pipes while retaining ownership for failure cleanup."""

    def __init__(self):
        self.processes = []
        self.pipes = []
        self.tasks = []
        self.errors = []
        self.clients = {}

    async def start(self, clients, args):
        context = multiprocessing.get_context("spawn")
        self.clients = {client.index: client for client in clients}
        count = min(args.workers, len(clients))
        for index in range(count):
            parent, child = context.Pipe()
            process = context.Process(target=worker_entry, args=(clients[index::count], args, child))
            process.start()
            child.close()
            self.pipes.append(parent)
            self.processes.append(process)
        for pipe in self.pipes:
            ready = await asyncio.wait_for(asyncio.to_thread(pipe.recv), args.startup_timeout)
            if ready.get("kind") != "ready":
                raise RuntimeError("Client worker failed to become ready")

    def begin(self, start_ns, end_ns, abort):
        for pipe in self.pipes:
            pipe.send((start_ns, end_ns))
            self.tasks.append(asyncio.create_task(self.receive(pipe, abort)))

    async def receive(self, pipe, abort):
        try:
            payload = await asyncio.to_thread(pipe.recv)
            if payload.get("kind") != "result":
                raise RuntimeError("Unexpected worker result")
            if payload["error"]:
                self.errors.append(payload["error"])
                abort.set()
            for state in payload["clients"]:
                client = self.clients[state["index"]]
                for key, value in state.items():
                    setattr(client, key, value)
                if client.error:
                    abort.set()
        except (OSError, EOFError, RuntimeError) as error:
            self.errors.append(f"Worker pipe: {error}")
            abort.set()

    async def finish(self, early, timeout):
        if early:
            for index, process in enumerate(self.processes):
                finished = index < len(self.tasks) and self.tasks[index].done()
                if process.is_alive() and not finished:
                    os.kill(process.pid, signal.SIGINT)
        try:
            if self.tasks:
                await asyncio.wait_for(asyncio.gather(*self.tasks), timeout)
        except asyncio.TimeoutError:
            self.errors.append("Timed out collecting client worker results")
        finally:
            for process in self.processes:
                if process.is_alive():
                    await asyncio.to_thread(process.join, .2)
                if process.is_alive():
                    process.terminate()
                    self.errors.append("Client worker required termination")
                await asyncio.to_thread(process.join, timeout)
                if process.is_alive():
                    process.kill()
                    await asyncio.to_thread(process.join)
                if process.exitcode != 0:
                    self.errors.append(f"Client worker exited with code {process.exitcode}")
            for pipe in self.pipes:
                pipe.close()
