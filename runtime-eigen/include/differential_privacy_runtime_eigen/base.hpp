
#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP


#include <release.pb.h>
#include <analysis.pb.h>

#include <Eigen/Dense>


extern "C" {

    char* release(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        char* dataPath, size_t dataPathLength,
        char* header, size_t headerLength);

    char* releaseArray(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        int m, int n, const double** data,
        char* header, size_t headerLength);
}

enum EvaluationDatatype {
    typeScalarNumeric, typeVectorNumeric
};

class RuntimeValue {
public:
    double valueScalar = 0;
    Eigen::VectorXd valueVector;
    EvaluationDatatype type;
    explicit RuntimeValue();
    explicit RuntimeValue(double value);
    explicit RuntimeValue(Eigen::VectorXd value);
    EvaluationDatatype getDatatype();
};

typedef std::map<unsigned int, std::map<std::string, RuntimeValue>> Evaluations;


Release* executeGraph(const Analysis& analysis, const Release& release,
                     const Eigen::MatrixXd& data, std::vector<std::string> columns);

std::map<std::string, RuntimeValue> executeComponent(const Component& component, const Evaluations& evaluations,
                                                     const Eigen::MatrixXd& data, std::vector<std::string> columns);
template<typename M>
M load_csv(const std::string & path);



Evaluations releaseToEvaluations(const Release& release);
Release* evaluationsToRelease(const Evaluations& evaluations);
#endif //DIFFERENTIAL_PRIVACY_BASE_HPP
