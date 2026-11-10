cmake_minimum_required(VERSION 4.3)
project(TickStream VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release when no build type is given
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

add_executable(TickStream src/main.cpp)
include(CTest)
option(TICKSTREAM_BUILD_TESTS "Build unit tests" ON)

if (TICKSTREAM_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()

# Main server executable
add_executable(tickstream_server src/main.cpp)
