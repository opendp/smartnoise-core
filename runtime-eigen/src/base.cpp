#include "../include/differential_privacy_runtime_eigen/base.hpp"

#include <iostream>

int release(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        int m, int n, const double** data, char** columns,
        char* responseBuffer, size_t responseLength) {

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

    // get column names from char pointers
    auto* colnames = new std::vector<std::string>();
    for (int i = 0; i < n; ++i) colnames->push_back(std::string(columns[i]));


    // DEBUGGING
    std::cout << "Analysis:\n" << analysisProto.DebugString();
    std::cout << "Release:\n" << releaseProto.DebugString();
    for (int i = 0; i < n; ++i) std::cout << columns[i] << ' ';
    std::cout << std::endl <<  matrix;


    // EXECUTION
    Release releaseProtoAfter = execute(analysisProto, releaseProto, matrix, *colnames);

    std::cout << "Release After:\n" << releaseProtoAfter.DebugString();

    std::string releaseMessage = releaseProtoAfter.SerializeAsString();

    google::protobuf::ShutdownProtobufLibrary();
    auto* responseBufferRaw = const_cast<char *>(releaseMessage.c_str());

    strncpy(responseBuffer, responseBufferRaw, responseLength);
    return releaseMessage.length();
}

Release execute(Analysis analysisProto, Release releaseProto, Eigen::MatrixXd data, std::vector<std::string> columns) {
    google::protobuf::Map<google::protobuf::uint32, Component> graph = analysisProto.graph();



    return releaseProto;
}