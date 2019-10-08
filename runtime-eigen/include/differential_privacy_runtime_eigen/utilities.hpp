
#ifndef DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_UTILITIES_HPP
#define DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_UTILITIES_HPP

#include <Eigen/Dense>

double sampleLaplace(double mu = 0, double scale = 1);
double sampleUniform(double low = 0, double high = 1);

#endif //DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_UTILITIES_HPP
