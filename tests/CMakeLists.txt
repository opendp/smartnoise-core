cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# project definition/versioning
project(differential_privacy_tests)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

# import libdifferential_privacy.so
add_library(differential_privacy SHARED IMPORTED)
set_property(TARGET differential_privacy
        PROPERTY IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/../base/cmake-build-debug/lib/libdifferential_privacy.so)

set(SOURCE_FILES
        src/main.cpp
        src/base/tests.cpp
#        src/runtime-eigen/tests.cpp
        )


set(PROTO_FILES
        ../prototypes/analysis.proto
        ../prototypes/release.proto
        ../prototypes/types.proto
        )

include(FindProtobuf)
find_package(Protobuf REQUIRED)

# check if protobuf was found
if(PROTOBUF_FOUND)
    message ("protobuf found")
else()
    message (FATAL_ERROR "Cannot find Protobuf")
endif()

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Print path to generated files
message ("PROTO_SRCS = ${PROTO_SRCS}")
message ("PROTO_HDRS = ${PROTO_HDRS}")

add_executable(tests ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS})


target_link_libraries(
        tests
        differential_privacy
        #        differential_privacy_runtime_eigen
        ${CONAN_LIBS}
)

include_directories(${PROJECT_SOURCE_DIR}/../base/include/)
#include_directories(${PROJECT_SOURCE_DIR}/../runtime-eigen/include/)

target_include_directories(tests
        PUBLIC
        ${PROTOBUF_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}
        )

find_package(Catch2 REQUIRED)
include(CTest)
include(Catch)
catch_discover_tests(tests)
