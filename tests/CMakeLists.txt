include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.18.0
)

set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(tickstream_tests protocol_test.cpp)
target_link_libraries(tickstream_tests PRIVATE GTest::gtest_main tickstream_core tickstream_build_options)
target_compile_features(tickstream_tests PRIVATE cxx_std_20)

include(GoogleTest)
gtest_discover_tests(tickstream_tests)
