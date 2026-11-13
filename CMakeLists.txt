cmake_minimum_required(VERSION 4.3)
project(TickStream VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release when no build type is given
if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build configuration" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif ()

include(CTest)
option(TICKSTREAM_BUILD_TESTS "Build unit tests" ON)
option(TICKSTREAM_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(tickstream_build_options INTERFACE)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(tickstream_build_options INTERFACE -Wall -Wextra -Wpedantic)
endif ()

if (TICKSTREAM_ENABLE_SANITIZERS)
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "Sanitizers require GCC or Clang")
    endif ()
    include(CheckCXXSourceCompiles)
    include(CMakePushCheckState)
    cmake_push_check_state(RESET)
    set(CMAKE_REQUIRED_FLAGS "-fsanitize=address,undefined")
    check_cxx_source_compiles("int main() { return 0; }" TICKSTREAM_SANITIZERS_SUPPORTED)
    cmake_pop_check_state()
    if (NOT TICKSTREAM_SANITIZERS_SUPPORTED)
        message(FATAL_ERROR
            "Sanitizer compile/link check failed. Install the AddressSanitizer and "
            "UndefinedBehaviorSanitizer runtime/development libraries for your compiler, "
            "or configure with -DTICKSTREAM_ENABLE_SANITIZERS=OFF.")
    endif ()
    target_compile_options(tickstream_build_options INTERFACE
        -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(tickstream_build_options INTERFACE -fsanitize=address,undefined)
endif ()

add_library(tickstream_core STATIC src/protocol.cpp src/transport.cpp src/market.cpp)
target_include_directories(tickstream_core PUBLIC include)
target_link_libraries(tickstream_core PRIVATE tickstream_build_options)

add_executable(tickstream_server src/main.cpp)
target_link_libraries(tickstream_server PRIVATE tickstream_core tickstream_build_options)

if (BUILD_TESTING AND TICKSTREAM_BUILD_TESTS)
    add_subdirectory(tests)
endif ()
