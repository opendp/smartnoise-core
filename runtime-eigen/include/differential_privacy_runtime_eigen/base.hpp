
#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP

#include <map>
#include <vector>
#include "../../../base/include/differential_privacy/components.hpp"
//#include "../../../../../.conan/data/eigen/3.3.7/conan/stable/package/5ab84d6acfe1f23c4fae0ab88f26e3a396351ac9/include/eigen3/Eigen/src/Core/Matrix.h"

void release(Component component, std::vector<double> data);
//Eigen::MatrixX2f release(Component component, std::map<std::string, std::map<std::string, std::vector<double>>> data);


#endif //DIFFERENTIAL_PRIVACY_BASE_HPP
