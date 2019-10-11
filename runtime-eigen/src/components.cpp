
#include "../include/differential_privacy_runtime_eigen/base.hpp"
#include "../include/differential_privacy_runtime_eigen/components.hpp"
#include "../include/differential_privacy_runtime_eigen/utilities.hpp"


NodeEvaluation componentAdd(RuntimeValue left, RuntimeValue right) {
    return NodeEvaluation({{"data", left + right}});
}

NodeEvaluation componentMean(RuntimeValue data) {
    return NodeEvaluation( {{"data", RuntimeValue(data.valueVector.mean())}});
}

NodeEvaluation componentLaplace(RuntimeValue data, RuntimeValue minimum, RuntimeValue maximum, RuntimeValue num_records, double epsilon) {
    double sensitivity = (maximum.valueScalar - minimum.valueScalar) / num_records.valueScalar;

    RuntimeValue runtimeValue(data.valueScalar + sampleLaplace(0, sensitivity / epsilon));
    return NodeEvaluation({{"data", runtimeValue}});
}

NodeEvaluation componentConstant(RuntimeValue value) {
    return NodeEvaluation({{"data", value}});
}

NodeEvaluation componentDPMeanLaplace(RuntimeValue data,
        RuntimeValue minimum, RuntimeValue maximum,
        RuntimeValue num_records, double epsilon) {

    RuntimeValue dataClipped = componentClip(data, minimum, maximum)["data"];
    RuntimeValue mean = componentMean(dataClipped)["data"];
    RuntimeValue noised = componentLaplace(dataClipped, minimum, maximum, num_records, epsilon)["data"];

    return NodeEvaluation({{"data", noised}});
}

NodeEvaluation componentClip(RuntimeValue data, RuntimeValue minimum, RuntimeValue maximum) {
    return NodeEvaluation({{"data", RuntimeValue(
            data.valueVector.array().max(maximum.valueScalar).min(minimum.valueScalar))
    }});
}
