#ifndef DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP
#define DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP

#include "components.hpp"

class Impute : public Transform {
protected:
    std::string _name = "impute";
public:
    explicit Impute(Component* child);
};

class Clip : public Transform {
protected:
    std::list<double> _bounds;
    std::string _name = "clip";
public:
    explicit Clip(Component* child, std::list<double> bounds);
};

#endif //DIFFERENTIAL_PRIVACY_TRANSFORMS_HPP
