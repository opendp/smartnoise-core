cmake_minimum_required(VERSION 3.14)
project(differential_privacy VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)

# initialize package manager
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

# fix dyld dynamic linker in mac
set(CMAKE_MACOSX_RPATH OFF)

# define files to use in build
file(GLOB SOURCE_FILES src/*.cpp)
file(GLOB HEADER_FILES include/differential_privacy/*.hpp)
file(GLOB PROTO_FILES ../prototypes/*.proto)

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
message ("PROTO_SRCS = ${PROTO_SRCS}")
message ("PROTO_HDRS = ${PROTO_HDRS}")

# build proto shared library
add_library(differential_privacy_proto SHARED
        ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(differential_privacy_proto PUBLIC
        ${PROTOBUF_INCLUDE_DIRS})
target_link_libraries(differential_privacy_proto PUBLIC
        ${PROTOBUF_LIBRARIES})
set_target_properties(differential_privacy_proto PROPERTIES
        VERSION ${PROJECT_VERSION})

# create shared library
add_library(differential_privacy SHARED
    ${SOURCE_FILES} ${HEADER_FILES})

target_include_directories(differential_privacy PUBLIC
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR})

# link libraries used from package manager
target_link_libraries(differential_privacy PUBLIC
    differential_privacy_proto
    ${CONAN_LIBS}
    ${PROTOBUF_LIBRARIES})

set_target_properties(differential_privacy PROPERTIES
    VERSION ${PROJECT_VERSION})
