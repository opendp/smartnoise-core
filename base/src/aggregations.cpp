#include "../include/differential_privacy/aggregations.hpp"

Mean::Mean(Component* child) : Aggregate(child) {}

std::string Mean::get_name() {
    return "mean";
}