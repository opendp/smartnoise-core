#include "../include/differential_privacy_runtime_eigen/base.hpp"
#include "../include/differential_privacy_runtime_eigen/components.hpp"
#include <differential_privacy/base.hpp>

#include <iostream>
#include <queue>
#include <fstream>
#include <stack>

burdock::Release* executeGraph(
        const burdock::Analysis& analysis, const burdock::Release& release,
        const Eigen::MatrixXd& data, std::vector<std::string> columns) {

    std::stack<unsigned int> traversal;
    std::set<unsigned int> nodeIdsRelease = getReleaseNodes(analysis);
    for (const auto& nodeId : getSinks(analysis)) traversal.push(nodeId);

    GraphEvaluation evaluations = releaseToEvaluations(release);
    google::protobuf::Map<unsigned int, burdock::Component> graph = analysis.graph();

    // track node parents
    std::map<unsigned int, std::set<unsigned int>> parents;
    for (const auto& nodePair : graph) {
        for (const auto& argumentPair : nodePair.second.arguments()) {
            unsigned int argumentNodeId = argumentPair.second.source_node_id();
            if (parents.find(argumentNodeId) == parents.end())
                parents[argumentNodeId] = std::set<unsigned int>();
            parents[argumentNodeId].insert(nodePair.first);
        }
    }

    while (!traversal.empty()) {
        unsigned int nodeId = traversal.top();

        auto arguments = graph[nodeId].arguments();
        auto it = arguments.begin();

        bool evaluable = true;
        while (evaluable && it != arguments.end()) {
            if (evaluations.find((*it).second.source_node_id()) != evaluations.end())
                evaluable = false;
        }

        // check if all arguments are available
        if (it == arguments.end()) {
            traversal.pop();

            // TODO evaluate node via evaluations map

            evaluations[nodeId] = executeComponent(graph[nodeId], evaluations, data, columns);

            // remove references to parent node, and if empty and private
            for (const auto& argumentPair : arguments) {
                unsigned int argumentNodeId = argumentPair.second.source_node_id();
                parents[argumentNodeId].erase(nodeId);
                if (parents[argumentNodeId].size() == 0) {
                    if (nodeIdsRelease.find(argumentNodeId) != nodeIdsRelease.end()) {
                        evaluations.erase(argumentNodeId);
                        // parents.erase(argumentNodeId); // optional
                    }
                }
            }
        }

    }
    return evaluationsToRelease(evaluations);
}

NodeEvaluation executeComponent(burdock::Component component,
                                const GraphEvaluation& evaluations,
                                const Eigen::MatrixXd &data, std::vector<std::string> columns) {

    auto arguments = component.mutable_arguments();

    if (component.has_datasource()) {
        burdock::DataSource datasource = component.datasource();
        auto it = std::find(columns.begin(), columns.end(), datasource.column_id());
        int index = std::distance(columns.begin(), it);
        RuntimeValue runtimeValue(data.col(index));
        return NodeEvaluation({{"data", runtimeValue}});
    }

    if (component.has_mean())
        return componentMean(getArgument(evaluations, arguments->at("data")));

    if (component.has_add()) {
        RuntimeValue left = getArgument(evaluations, arguments->at("left"));
        RuntimeValue right = getArgument(evaluations, arguments->at("right"));
        return componentAdd(left, right);
    }

    if (component.has_literal()) {
        auto literalProto = component.literal();

        if (literalProto.has_ndarray()) {
            // TODO: unwrap ndarray from protobuf. Just assuming len(shape) == 1
//            auto dataProto = literalProto.ndarray().data();
//            Eigen::VectorXd dataVector = {dataProto.begin(), dataProto.end()};
//            runtimeValue = new RuntimeValue(dataVector);
        }
        else {
            return NodeEvaluation({{"data", RuntimeValue(literalProto.numeric())}});
        }
    }

    if (component.has_dpmeanlaplace()) {
        double epsilon = component.dpmeanlaplace().epsilon();
        RuntimeValue valueData = getArgument(evaluations, arguments->at("data"));
        RuntimeValue valueN = getArgument(evaluations, arguments->at("num_records"));
        RuntimeValue valueMin = getArgument(evaluations, arguments->at("minimum"));
        RuntimeValue valueMax = getArgument(evaluations, arguments->at("maximum"));
        return componentDPMeanLaplace(valueData, valueMin, valueMax, valueN, epsilon);
    }

    if (component.has_laplace()) {
        double epsilon = component.laplace().epsilon();
        RuntimeValue valueData = getArgument(evaluations, arguments->at("data"));
        RuntimeValue valueN = getArgument(evaluations, arguments->at("num_records"));
        RuntimeValue valueMin = getArgument(evaluations, arguments->at("minimum"));
        RuntimeValue valueMax = getArgument(evaluations, arguments->at("maximum"));
        return componentLaplace(valueData, valueMin, valueMax, valueN, epsilon);
    }

    return std::map<std::string, RuntimeValue>();
}

RuntimeValue getArgument(GraphEvaluation graphEvaluation, burdock::Component::Field argument) {
    return graphEvaluation[argument.source_node_id()][argument.source_field()];
}

RuntimeValue::RuntimeValue() {}
RuntimeValue::RuntimeValue(double value) {
    this->valueScalar = value;
    this->type = typeScalarNumeric;
}

RuntimeValue::RuntimeValue(Eigen::VectorXd value) {
    this->valueVector = value;
    this->type = typeVectorNumeric;
}

EvaluationDatatype RuntimeValue::getDatatype() {
    return this->type;
}
RuntimeValue RuntimeValue::operator+(RuntimeValue right) {
    // TODO: code small here, enumerating all the cases, but my C++ is rusty
    if (this->getDatatype() == EvaluationDatatype::typeScalarNumeric && right.getDatatype() == EvaluationDatatype::typeScalarNumeric) {
        return RuntimeValue(this->valueScalar + right.valueScalar);
    }
    else if (this->getDatatype() == EvaluationDatatype::typeScalarNumeric && right.getDatatype() == EvaluationDatatype::typeVectorNumeric) {
        return RuntimeValue(this->valueScalar + right.valueVector.array());
    }
    else if (this->getDatatype() == EvaluationDatatype::typeVectorNumeric && right.getDatatype() == EvaluationDatatype::typeVectorNumeric) {
        return RuntimeValue(this->valueVector + right.valueVector);
    }
    else if (this->getDatatype() == EvaluationDatatype::typeVectorNumeric && right.getDatatype() == EvaluationDatatype::typeScalarNumeric) {
        return RuntimeValue(this->valueVector.array() + right.valueScalar);
    }
    throw std::invalid_argument("RuntimeValue type is not handled.");
}

GraphEvaluation releaseToEvaluations(const burdock::Release& release) {
    GraphEvaluation evaluations;

    for (std::pair<unsigned int, burdock::ReleaseNode> releaseNodePair : release.values()) {
        burdock::ReleaseNode releaseNode = releaseNodePair.second;

        for (std::pair<std::string, burdock::Value> valuePair : releaseNode.values()) {
            burdock::Value value = valuePair.second;
            if (value.type() == burdock::DataType::scalar_numeric)
                evaluations[releaseNodePair.first][valuePair.first] = RuntimeValue(value.scalar_numeric());;

            // TODO: read in other types of values
        }
    }

    return evaluations;
}

burdock::Release* evaluationsToRelease(const GraphEvaluation& evaluations) {
    auto* release = new burdock::Release();
    auto* releaseValues = release->mutable_values();

    for (const auto& evaluatedNodePair : evaluations) {
        unsigned int nodeId = evaluatedNodePair.first;
        burdock::ReleaseNode releaseNode;
        auto releaseNodeValues = releaseNode.mutable_values();

        std::map<std::string, RuntimeValue> evaluatedNodeValues = evaluatedNodePair.second;

        for (const auto& valuePair : evaluatedNodeValues) {
            std::string argumentName = valuePair.first;
            RuntimeValue runtimeValue = valuePair.second;

            if (runtimeValue.getDatatype() == EvaluationDatatype::typeScalarNumeric) {
                burdock::Value value;
                value.set_scalar_numeric(runtimeValue.valueScalar);
                (*releaseNodeValues)[argumentName] = value;
            }
            // TODO: read in other types of values
        }
        (*releaseValues)[nodeId] = releaseNode;
    }

    return release;
}


Eigen::MatrixXd load_csv(const std::string & path) {
    std::ifstream indata;
    indata.open(path);
    std::string line;
    std::vector<double> values;
    uint rows = 0;
    while (std::getline(indata, line)) {
        std::stringstream lineStream(line);
        std::string cell;
        while (std::getline(lineStream, cell, ',')) {
            values.push_back(std::stod(cell));
        }
        ++rows;
    }

    typedef const Eigen::Matrix<
            typename Eigen::MatrixXd::Scalar,
            Eigen::MatrixXd::RowsAtCompileTime,
            Eigen::MatrixXd::ColsAtCompileTime,
            Eigen::RowMajor> MatrixCSV;

    return Eigen::Map<MatrixCSV>(values.data(), rows, values.size()/rows);
}