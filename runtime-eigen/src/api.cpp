#include "../include/differential_privacy_runtime_eigen/api.hpp"
#include "../include/differential_privacy_runtime_eigen/base.hpp"

extern "C" char* release(
        char* datasetBuffer, size_t datasetLength,
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength) {

    // parse analysis from protocol buffer
    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysisProto;
    analysisProto.ParseFromString(analysisString);

    std::string releaseString(releaseBuffer, releaseLength);
    burdock::Release releaseProto;
    releaseProto.ParseFromString(releaseString);

    std::string datasetString(datasetBuffer, datasetLength);
    burdock::Release datasetProto;
    datasetProto.ParseFromString(datasetString);

    // TODO: pull dataPathString from datasetProto
    // TODO: pull columns from datasetProto or file
    // construct eigen matrix from double pointers
    auto matrix = load_csv(dataPathString);

    // DEBUGGING
    std::cout << "Analysis:\n" << analysisProto.DebugString();
    std::cout << "Release:\n" << releaseProto.DebugString();
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

void free_ptr(char* ptr) {
    free(ptr);
}