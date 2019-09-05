#ifndef DIFFERENTIAL_PRIVACY_MECHANISMS_HPP
#define DIFFERENTIAL_PRIVACY_MECHANISMS_HPP

#include "base.hpp"

class Laplace : public Mechanism {
public:
    Laplace(Component child);
};

#endif //DIFFERENTIAL_PRIVACY_MECHANISMS_HPP
