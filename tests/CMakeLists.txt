cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# project definition/versioning
project(differential_privacy_tests)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

include_directories(/home/shoe/CLionProjects/differential_privacy/base/include/)
include_directories(/home/shoe/CLionProjects/differential_privacy/runtime-eigen/include/)

set(SOURCE_FILES
        src/main.cpp
        src/base/tests.cpp
        src/runtime-eigen/tests.cpp
        )

add_executable(tests ${SOURCE_FILES})
target_link_libraries(tests differential_privacy differential_privacy_runtime_eigen ${CONAN_LIBS})

find_package(Catch2 REQUIRED)
include(CTest)
include(Catch)
catch_discover_tests(tests)
