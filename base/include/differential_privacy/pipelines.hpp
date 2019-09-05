#ifndef DIFFERENTIAL_PRIVACY_PIPELINES_HPP
#define DIFFERENTIAL_PRIVACY_PIPELINES_HPP

#include "base.hpp"
#include "mechanisms.hpp"

Laplace DPMean(Component child, std::list<double> bounds);

#endif //DIFFERENTIAL_PRIVACY_PIPELINES_HPP
