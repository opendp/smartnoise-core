
#include <utility>

#include "../include/differential_privacy/transforms.hpp"
#include "../include/differential_privacy/aggregations.hpp"
#include "../include/differential_privacy/mechanisms.hpp"

Laplace DPMean(Component child, std::list<double> bounds) {
    return Laplace(Mean(Clip(Impute(std::move(child)), std::move(bounds))));
}
