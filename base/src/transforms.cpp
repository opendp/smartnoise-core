#include "../include/differential_privacy/transforms.hpp"

#include <utility>
#include "../include/differential_privacy/base.hpp"

Clip::Clip(Component child, std::list<double> bounds) : Transform(std::move(child)), bounds{std::move(bounds)} {}
Impute::Impute(Component child) : Transform(std::move(child)) {}
