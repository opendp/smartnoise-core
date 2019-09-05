cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)

# uncomment to build against python version. Must update path to python executable
#set(PYTHON_EXECUTABLE /home/shoe/anaconda3/bin/python)
#set(PYBIND11_PYTHON_VERSION 3.7)

# project definition/versioning
project(differential_privacy_bindings_python)

# project dependencies
include(${PROJECT_SOURCE_DIR}/build/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

# create module
find_package(pybind11 REQUIRED)
pybind11_add_module(bindings_python MODULE src/bindings.cpp)
target_link_libraries(bindings_python ${CONAN_LIBS})
