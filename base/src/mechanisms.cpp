//
// Created by shoe on 9/4/19.
//

#include "../include/differential_privacy/mechanisms.hpp"
#include "../include/differential_privacy/base.hpp"
#include <utility>

Laplace::Laplace(Component child) : Mechanism(std::move(child)) {}