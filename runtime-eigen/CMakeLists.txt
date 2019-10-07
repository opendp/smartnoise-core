cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# project definition/versioning
project(differential_privacy_runtime_eigen)

# 1. SETUP RUNTIME-EIGEN
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

set(SOURCE_FILES
        src/api.cpp
        src/base.cpp)

set(HEADER_FILES
        include/differential_privacy_runtime_eigen/api.hpp
        include/differential_privacy_runtime_eigen/base.hpp)

# 2. SETUP VALIDATOR LIBRARY
# import compiled sources libdifferential_privacy.so
add_library(differential_privacy SHARED IMPORTED)
set_property(TARGET differential_privacy
        PROPERTY IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy.so)
# include headers
include_directories(${PROJECT_SOURCE_DIR}/../validator-c++/include/)

# 3. SETUP PROTOBUF
set(PROTO_FILES
        ../prototypes/analysis.proto
        ../prototypes/release.proto
        ../prototypes/types.proto)

include(FindProtobuf)
find_package(Protobuf 3.9.0 REQUIRED)

# check if protobuf was found
if(PROTOBUF_FOUND)
    message ("protobuf found")
else()
    message (FATAL_ERROR "Cannot find Protobuf")
endif()

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Print path to generated files
#message ("PROTO_SRCS = ${PROTO_SRCS}")
#message ("PROTO_HDRS = ${PROTO_HDRS}")

# 4. MAKE TARGET
add_library(differential_privacy_runtime_eigen SHARED
        ${SOURCE_FILES} ${HEADER_FILES}
        ${PROTO_SRCS} ${PROTO_HDRS}
        )

#message(${PROTOBUF_INCLUDE_DIRS})
target_include_directories(differential_privacy_runtime_eigen PUBLIC
        ${PROTOBUF_INLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(differential_privacy_runtime_eigen PUBLIC
        differential_privacy
        ${PROTOBUF_LIBRARIES}
        ${CONAN_LIBS})

