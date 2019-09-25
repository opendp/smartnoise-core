## INCOMPLETE
# CRAN doesn't permit compiled code, code must be built with the R toolchain. 
# CMake seems to be an issue with CRAN? 
# I was thinking a shim could be written in Rcpp (under /src) that does the SEXP wrapping.
# I have makefiles in the /src dir to prevent the typical R compilation, 
#  and a custom configure script that points to this CMake script
# Still not sure how the pieces link together with the R toolchain
# An alternative might be to create a package/install 
#  for the base differential privacy library/default runtime instead of building with?

# below is a partial mixture of the CMakeLists.txt of the base library with the shim


cmake_minimum_required(VERSION 3.14)
project(differential_privacy_R_shim VERSION 0.1.0)


set(CMAKE_CXX_STANDARD 17)

include(GenerateExportHeader)
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(dp_R_shim SHARED src/bindings.cpp)
generate_export_header(dp_R_shim)

install(TARGETS dp_R_shim DESTINATION lib)


# initialize package manager
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

# define files to use in build
set(SOURCE_FILES
        ../base/src/graph.cpp
        )
set(HEADER_FILES
        ../base/include/differential_privacy/graph.hpp
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

# create shared library
add_library(differential_privacy SHARED ${PROTO_SRCS} ${PROTO_HDRS} ${SOURCE_FILES} ${HEADER_FILES})


target_include_directories(differential_privacy
        PUBLIC
        ${PROTOBUF_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}
        )

# link libraries used from package manager
target_link_libraries(differential_privacy PUBLIC ${CONAN_LIBS} ${PROTOBUF_LIBRARIES})

set_target_properties(differential_privacy PROPERTIES VERSION ${PROJECT_VERSION})
