cmake_minimum_required(VERSION 3.14)
project(differential_privacy VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)

# initialize package manager
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup()

# define files to use in build
set(SOURCE_FILES
        src/aggregations.cpp
        src/components.cpp
        src/mechanisms.cpp
        src/pipelines.cpp
        src/privacy_definition.cpp
        src/transforms.cpp
        )
set(HEADER_FILES
        include/differential_privacy/aggregations.hpp
        include/differential_privacy/components.hpp
        include/differential_privacy/mechanisms.hpp
        include/differential_privacy/pipelines.hpp
        include/differential_privacy/privacy_definition.hpp
        include/differential_privacy/transforms.hpp
        )

# create shared library
add_library(differential_privacy SHARED ${SOURCE_FILES} ${HEADER_FILES})
# link libraries used from package manager
target_link_libraries(differential_privacy ${CONAN_LIBS})

set_target_properties(differential_privacy PROPERTIES VERSION ${PROJECT_VERSION})
