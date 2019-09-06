#include <pybind11/pybind11.h>
#include "../../base/include/differential_privacy/components.hpp"

// following examples from:
// https://pybind11.readthedocs.io/en/stable/classes.html
PYBIND11_MODULE(bindings_python, m) {
    m.doc() = "differential privacy python module";
    pybind11::class_<PrivacyDefinition>(m, "PrivacyDefinition")
            .def(pybind11::init<const std::string &>())
            .def_readwrite("function", &PrivacyDefinition::function);
}
