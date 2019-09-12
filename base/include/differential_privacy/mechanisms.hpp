#ifndef DIFFERENTIAL_PRIVACY_MECHANISMS_HPP
#define DIFFERENTIAL_PRIVACY_MECHANISMS_HPP

#include "components.hpp"

class Laplace : public Mechanism {
public:
    std::string get_name() override;
    explicit Laplace(Component* child);
};

#endif //DIFFERENTIAL_PRIVACY_MECHANISMS_HPP
