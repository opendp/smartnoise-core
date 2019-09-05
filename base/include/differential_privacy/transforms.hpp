#ifndef DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP
#define DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP

#include "base.hpp"
#include <Eigen/Dense>

class Impute : public Transform {
public:
    explicit Impute(Component child);
};

class Clip : public Transform {
    std::list<double> bounds;
public:
    explicit Clip(Component child, std::list<double> bounds);
};

#endif //DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP
