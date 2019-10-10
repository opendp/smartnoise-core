cmake_minimum_required(VERSION 3.14)
project(differential_privacy_tests)

set(CMAKE_CXX_STANDARD 17)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

if (UNIX AND NOT APPLE)
    set(EXTENSION .so)
endif()

if (APPLE)
    set(EXTENSION .dylib)
endif()

if (WIN32)
    set(EXTENSION .dll)
endif()

# fix dyld dynamic linker in mac
set(CMAKE_MACOSX_RPATH OFF)


# import protobuf definitions
add_library(differential_privacy_proto SHARED IMPORTED)
message(${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto${EXTENSION})
set_property(TARGET differential_privacy_proto
        PROPERTY IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto${EXTENSION})

# import validator
add_library(differential_privacy SHARED IMPORTED)
set_property(TARGET differential_privacy
        PROPERTY IMPORTED_LOCATION
        ${PROJECT_SOURCE_DIR}/../validator-c++/cmake-build-debug/lib/libdifferential_privacy${EXTENSION})

# import reference runtime
add_library(differential_privacy_runtime_eigen SHARED IMPORTED)
set_property(TARGET differential_privacy_runtime_eigen
        PROPERTY IMPORTED_LOCATION
        ${PROJECT_SOURCE_DIR}/../runtime-eigen/cmake-build-debug/lib/libdifferential_privacy_runtime_eigen${EXTENSION})

include_directories(${PROJECT_SOURCE_DIR}/../validator-c++/include/)
include_directories(${PROJECT_SOURCE_DIR}/../runtime-eigen/include/)

file(GLOB SOURCE_FILES src/validator-c++/*.cpp src/runtime-eigen/*.cpp src/*.cpp)
file(GLOB HEADER_FILES include/tests/*.hpp)
file(GLOB PROTO_FILES ../prototypes/*.proto)

include(FindProtobuf)
find_package(Protobuf 3.9.0 REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
file(GLOB PROTO_FILES ../prototypes/*.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_executable(tests
        ${SOURCE_FILES} ${HEADER_FILES} ${PROTO_HDRS})

target_include_directories(tests PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(tests PUBLIC
        differential_privacy_proto
        differential_privacy
        differential_privacy_runtime_eigen
        ${CONAN_LIBS})

find_package(Catch2 REQUIRED)
include(CTest)
include(Catch)
catch_discover_tests(tests)
