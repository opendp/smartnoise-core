cmake_minimum_required(VERSION 3.14)

# project definition/versioning
project(differential_privacy_runtime_eigen)

set(CMAKE_CXX_STANDARD 17)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

set(SOURCE_FILES
        src/base.cpp
        src/components.cpp
        )
set(HEADER_FILES
        include/differential_privacy_runtime_eigen/base.hpp
        include/differential_privacy_runtime_eigen/components.hpp
        )

add_library(differential_privacy_runtime_eigen SHARED ${SOURCE_FILES} ${HEADER_FILES})

target_link_libraries(differential_privacy_runtime_eigen differential_privacy ${CONAN_LIBS})
