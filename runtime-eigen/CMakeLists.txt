cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# project definition/versioning
project(differential_privacy_runtime_eigen)

# 1. SETUP RUNTIME-EIGEN
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

# fix dyld dynamic linker in mac
set(CMAKE_MACOSX_RPATH OFF)

file(GLOB SOURCE_FILES src/*.cpp)
file(GLOB HEADER_FILES include/differential_privacy_runtime_eigen/*.hpp)

if (UNIX AND NOT APPLE)
    set(EXTENSION .so)
endif()

if (APPLE)
    set(EXTENSION .dylib)
endif()

if (WIN32)
    set(EXTENSION .dll)
endif()

# 2. SETUP VALIDATOR LIBRARY
# import compiled sources libdifferential_privacy.so
add_library(differential_privacy SHARED IMPORTED)
message(${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy${EXTENSION})
set_property(TARGET differential_privacy
        PROPERTY IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy${EXTENSION})
# include headers
include_directories(${PROJECT_SOURCE_DIR}/../validator-c++/include/)

add_library(differential_privacy_proto SHARED IMPORTED)
message(${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto${EXTENSION})
set_property(TARGET differential_privacy_proto
        PROPERTY IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto${EXTENSION})


include(FindProtobuf)
find_package(Protobuf 3.9.0 REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
file(GLOB PROTO_FILES ../prototypes/*.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# 4. MAKE TARGET
add_library(differential_privacy_runtime_eigen SHARED
        ${SOURCE_FILES} ${HEADER_FILES} ${PROTO_HDRS})

#message(${PROTOBUF_INCLUDE_DIRS})
target_include_directories(differential_privacy_runtime_eigen PUBLIC
        ${PROTOBUF_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(differential_privacy_runtime_eigen PUBLIC
        differential_privacy_proto
        differential_privacy
        ${PROTOBUF_LIBRARIES}
        ${CONAN_LIBS})

