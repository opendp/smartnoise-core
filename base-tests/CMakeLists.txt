cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# project definition/versioning
project(differential_privacy_tests)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

include_directories(/home/shoe/CLionProjects/differential_privacy/base/include/)

add_executable(tests src/tests.cpp)
target_link_libraries(tests differential_privacy)
target_link_libraries(tests differential_privacy ${CONAN_LIBS})

find_package(Catch2 REQUIRED)
include(CTest)
include(Catch)
catch_discover_tests(tests)
