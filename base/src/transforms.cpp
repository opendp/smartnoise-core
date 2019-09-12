#include "../include/differential_privacy/transforms.hpp"

#include <utility>
#include "../include/differential_privacy/components.hpp"

Clip::Clip(Component* child, std::list<double> bounds) : Transform(child), _bounds{std::move(bounds)} {}
Impute::Impute(Component* child) : Transform(child) {}
