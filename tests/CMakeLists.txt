include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.18.0
)

set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(tickstream_tests protocol_test.cpp transport_test.cpp market_test.cpp)
target_link_libraries(tickstream_tests PRIVATE GTest::gtest_main tickstream_core tickstream_build_options)
target_compile_features(tickstream_tests PRIVATE cxx_std_20)
target_compile_definitions(tickstream_tests PRIVATE
    PROTOCOL_FIXTURE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/fixtures/protocol_v1.txt")

include(GoogleTest)
gtest_discover_tests(tickstream_tests PROPERTIES TIMEOUT 10)

find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter)
add_test(NAME protocol_python
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/protocol_python_test.py)
add_test(NAME server_cli
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/server_cli_test.py $<TARGET_FILE:tickstream_server>)
set_tests_properties(protocol_python server_cli PROPERTIES TIMEOUT 15)
