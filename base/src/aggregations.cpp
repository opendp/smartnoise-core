#include "../include/differential_privacy/aggregations.hpp"

#include <utility>

Mean::Mean(Component child) : Aggregate(std::move(child)) {}
