#include "../include/differential_privacy_runtime_eigen/base.hpp"
#include "../../base/include/differential_privacy/graph.hpp"

#include <iostream>
#include <queue>
#include <fstream>
#include <stack>

#include <boost/graph/directed_graph.hpp>

extern "C" char* release(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        char* dataPath, size_t dataPathLength,
        char* header, size_t headerLength) {

    // parse analysis from protocol buffer
    std::string analysisString(analysisBuffer, analysisLength);
    Analysis analysisProto;
    analysisProto.ParseFromString(analysisString);

    std::string releaseString(releaseBuffer, releaseLength);
    Release releaseProto;
    releaseProto.ParseFromString(releaseString);

    // construct eigen matrix from double pointers
    auto matrix = load_csv<Eigen::MatrixXd>(std::string(dataPath, dataPathLength));

    // get column names from char pointer
    auto* columns = new std::vector<std::string>();
    std::string headerStr(header, headerLength);

    size_t position = 0;
    std::string delimiter(",");

    while ((position = headerStr.find(delimiter)) != std::string::npos) {
        columns->push_back(headerStr.substr(0, position));
        headerStr.erase(0, position + delimiter.length());
    }

    // DEBUGGING
    std::cout << "Analysis:\n" << analysisProto.DebugString();
    std::cout << "Release:\n" << releaseProto.DebugString();
    for (const auto & column : *columns) std::cout << column << ' ';
    std::cout << std::endl <<  matrix;


    // EXECUTION
    Release* releaseProtoAfter = executeGraph(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter->DebugString();

    std::string releaseMessage = releaseProtoAfter->SerializeAsString();

    google::protobuf::ShutdownProtobufLibrary();
    return const_cast<char *>(releaseMessage.c_str());

//    strncpy(responseBuffer, responseBufferRaw, responseLength);
//    return releaseMessage.length();
}

extern "C" char* releaseArray(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        int m, int n, const double** data,
        char* header, size_t headerLength) {

    // parse analysis from protocol buffer
    std::string analysisString(analysisBuffer, analysisLength);
    Analysis analysisProto;
    analysisProto.ParseFromString(analysisString);

    std::string releaseString(releaseBuffer, releaseLength);
    Release releaseProto;
    releaseProto.ParseFromString(releaseString);

    // construct eigen matrix from double pointers
    Eigen::MatrixXd matrix(m, n);
    for (unsigned int i = 0; i < m; ++i)
        for (unsigned int j = 0; j < n; ++j)
            matrix(i, j) = data[i][j];

    // get column names from char pointer
    auto* columns = new std::vector<std::string>();
    std::string headerStr(header, headerLength);

    size_t position = 0;
    std::string delimiter(",");

    while ((position = headerStr.find(delimiter)) != std::string::npos) {
        columns->push_back(headerStr.substr(0, position));
        headerStr.erase(0, position + delimiter.length());
    }

    // DEBUGGING
    std::cout << "Analysis:\n" << analysisProto.DebugString();
    std::cout << "Release:\n" << releaseProto.DebugString();
    for (const auto & column : *columns) std::cout << column << ' ';
    std::cout << std::endl <<  matrix;

    // EXECUTION
    Release* releaseProtoAfter = executeGraph(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter->DebugString();

    std::string releaseMessage = releaseProtoAfter->SerializeAsString();

    google::protobuf::ShutdownProtobufLibrary();
    return const_cast<char *>(releaseMessage.c_str());

//    strncpy(responseBuffer, responseBufferRaw, responseLength);
//    return releaseMessage.length();
}

Release* executeGraph(
        const Analysis& analysis, const Release& release,
        const Eigen::MatrixXd& data, std::vector<std::string> columns) {

    std::stack<unsigned int> traversal;
    std::set<unsigned int> nodeIdsRelease = getReleaseNodes(analysis);
    for (const auto& nodeId : getSinks(analysis)) traversal.push(nodeId);

    Evaluations evaluations = releaseToEvaluations(release);
    google::protobuf::Map<unsigned int, Component> graph = analysis.graph();

    // track node parents
    std::map<unsigned int, std::set<unsigned int>> parents;
    for (const auto& nodePair : graph) {
        for (const Argument& argument : nodePair.second.arguments()) {
            unsigned int argumentNodeId = argument.node_id();
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
            if (evaluations.find((*it).node_id()) != evaluations.end())
                evaluable = false;
        }

        // check if all arguments are available
        if (it == arguments.end()) {
            traversal.pop();

            // TODO evaluate node via evaluations map

            evaluations[nodeId] = executeComponent(graph[nodeId], evaluations, data, columns);

            // remove references to parent node, and if empty and private
            for (const Argument& argument : arguments) {
                unsigned int argumentNodeId = argument.node_id();
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

std::map<std::string, RuntimeValue> executeComponent(const Component& component, const Evaluations& evaluations,
                                                     const Eigen::MatrixXd& data, std::vector<std::string> columns) {
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

Evaluations releaseToEvaluations(const Release& release) {
    Evaluations evaluations;

    for (std::pair<unsigned int, ReleaseNode> releaseNodePair : release.values()) {
        ReleaseNode releaseNode = releaseNodePair.second;

        for (std::pair<std::string, Value> valuePair : releaseNode.values()) {
            Value value = valuePair.second;
            if (value.type() == DataType::scalar_numeric)
                evaluations[releaseNodePair.first][valuePair.first] = RuntimeValue(value.scalar_numeric());;

            // TODO: read in other types of values
        }
    }

    return evaluations;
}

Release* evaluationsToRelease(const Evaluations& evaluations) {
    auto* release = new Release();
    auto* releaseValues = release->mutable_values();

    for (const auto& evaluatedNodePair : evaluations) {
        unsigned int nodeId = evaluatedNodePair.first;
        ReleaseNode releaseNode;
        auto releaseNodeValues = releaseNode.mutable_values();

        std::map<std::string, RuntimeValue> evaluatedNodeValues = evaluatedNodePair.second;

        for (const auto& valuePair : evaluatedNodeValues) {
            std::string argumentName = valuePair.first;
            RuntimeValue runtimeValue = valuePair.second;

            if (runtimeValue.getDatatype() == EvaluationDatatype::typeScalarNumeric) {
                Value value;
                value.set_scalar_numeric(runtimeValue.valueScalar);
                (*releaseNodeValues)[argumentName] = value;
            }
            // TODO: read in other types of values
        }
        (*releaseValues)[nodeId] = releaseNode;
    }

    return release;
}


template<typename M>
M load_csv(const std::string & path) {
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
            typename M::Scalar,
            M::RowsAtCompileTime,
            M::ColsAtCompileTime,
            Eigen::RowMajor> MatrixCSV;

    return Eigen::Map<MatrixCSV>(values.data(), rows, values.size()/rows);
}