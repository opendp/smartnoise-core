#include "../include/differential_privacy/mechanisms.hpp"
#include "../include/differential_privacy/components.hpp"

Laplace::Laplace(Component* child) : Mechanism(child) {}
std::string Laplace::get_name() {
    return "laplace";
}