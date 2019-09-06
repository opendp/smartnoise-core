#ifndef DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP
#define DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP

#include "components.hpp"

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
