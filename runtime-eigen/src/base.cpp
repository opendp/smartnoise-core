#include "../include/differential_privacy_runtime_eigen/base.hpp"
#include "../../base/include/differential_privacy/graph.hpp"

#include <iostream>
#include <queue>
#include <fstream>

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
    Release releaseProtoAfter = execute(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter.DebugString();

    std::string releaseMessage = releaseProtoAfter.SerializeAsString();

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
    Release releaseProtoAfter = execute(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter.DebugString();

    std::string releaseMessage = releaseProtoAfter.SerializeAsString();

    google::protobuf::ShutdownProtobufLibrary();
    return const_cast<char *>(releaseMessage.c_str());

//    strncpy(responseBuffer, responseBufferRaw, responseLength);
//    return releaseMessage.length();
}

Release execute(
        const Analysis& analysis, const Release& release,
        const Eigen::MatrixXd& data, std::vector<std::string> columns) {

    std::set<unsigned int> sinkIds = getSinks(analysis);
    std::queue<unsigned int, std::deque<unsigned int>> nodeQueue(
            std::deque<unsigned int>(sinkIds.begin(), sinkIds.end()));

    DirectedGraph graph = toGraph(analysis);
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