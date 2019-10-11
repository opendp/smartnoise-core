#ifndef DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_COMPONENTS_HPP
#define DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_COMPONENTS_HPP

#include "base.hpp"

NodeEvaluation componentLaplace(RuntimeValue data, RuntimeValue minimum, RuntimeValue maximum, RuntimeValue num_records, double epsilon);
NodeEvaluation componentDPMeanLaplace(RuntimeValue data, RuntimeValue minimum, RuntimeValue maximum, RuntimeValue num_records, double epsilon);
NodeEvaluation componentClip(RuntimeValue data, RuntimeValue minimum, RuntimeValue maximum);
NodeEvaluation componentMean(RuntimeValue data);
NodeEvaluation componentAdd(RuntimeValue left, RuntimeValue right);

#endif //DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_COMPONENTS_HPP
