
#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP


#include <release.pb.h>
#include <analysis.pb.h>

#include <Eigen/Dense>

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
    RuntimeValue operator+(RuntimeValue right);
};

typedef std::map<std::string, RuntimeValue> NodeEvaluation;
typedef std::map<unsigned int, NodeEvaluation> GraphEvaluation;

RuntimeValue getArgument(GraphEvaluation graphEvaluation, burdock::Component::Field argument);


burdock::Release* executeGraph(const burdock::Analysis& analysis, const burdock::Release& release,
                     const Eigen::MatrixXd& data, std::vector<std::string> columns);

std::map<std::string, RuntimeValue> executeComponent(burdock::Component component, const GraphEvaluation& evaluations,
                                                     const Eigen::MatrixXd& data, std::vector<std::string> columns);
Eigen::MatrixXd load_csv(const std::string & path);



GraphEvaluation releaseToEvaluations(const burdock::Release& release);
burdock::Release* evaluationsToRelease(const GraphEvaluation& evaluations);
#endif //DIFFERENTIAL_PRIVACY_BASE_HPP
