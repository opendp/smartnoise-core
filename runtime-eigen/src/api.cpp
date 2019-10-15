#include "../include/differential_privacy_runtime_eigen/api.hpp"
#include "../include/differential_privacy_runtime_eigen/base.hpp"

extern "C" char* release(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        char* dataPath, size_t dataPathLength,
        char* header, size_t headerLength) {

    // parse analysis from protocol buffer
    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysisProto;
    analysisProto.ParseFromString(analysisString);

    std::string releaseString(releaseBuffer, releaseLength);
    burdock::Release releaseProto;
    releaseProto.ParseFromString(releaseString);

    std::string dataPathString(dataPath, dataPathLength);

    // construct eigen matrix from double pointers
    auto matrix = load_csv(dataPathString);

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
    burdock::Release* releaseProtoAfter = executeGraph(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter->DebugString();

    std::string releaseMessage = releaseProtoAfter->SerializeAsString();

//    shutting down protobufs are picky in dlls. needs testing
//    google::protobuf::ShutdownProtobufLibrary();

    // NOTE: call lib_dp.freePtr(char* ptr) to free the duplicate string from memory
    return strdup(releaseMessage.c_str());
}

extern "C" char* releaseArray(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        int m, int n, const double** data,
        char* header, size_t headerLength) {

    // parse analysis from protocol buffer
    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysisProto;
    analysisProto.ParseFromString(analysisString);

    std::string releaseString(releaseBuffer, releaseLength);
    burdock::Release releaseProto;
    releaseProto.ParseFromString(releaseString);

    // construct eigen matrix from double pointers
    Eigen::MatrixXd matrix(m, n);
    for (unsigned int i = 0; i < m; ++i)
        for (unsigned int j = 0; j < n; ++j)
            matrix(i, j) = data[i][j];

//    std::cout << matrix << std::endl;

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
    burdock::Release* releaseProtoAfter = executeGraph(analysisProto, releaseProto, matrix, *columns);

    std::cout << "Release After:\n" << releaseProtoAfter->DebugString();

    std::string releaseMessage = releaseProtoAfter->SerializeAsString();

//    google::protobuf::ShutdownProtobufLibrary();
    return strdup(releaseMessage.c_str());
}

void freePtr(char* ptr) {
    free(ptr);
}