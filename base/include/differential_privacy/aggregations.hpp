#ifndef DIFFERENTIAL_PRIVACY_AGGREGATIONS_HPP
#define DIFFERENTIAL_PRIVACY_AGGREGATIONS_HPP

#include "components.hpp"

class Mean : public Aggregate {
public:
    Mean(Component child);
};

#endif //DIFFERENTIAL_PRIVACY_AGGREGATIONS_HPP
