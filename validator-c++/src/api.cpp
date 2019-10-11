#include "analysis.pb.h"
#include <iostream>

#include "../include/differential_privacy/api.hpp"
#include "../include/differential_privacy/base.hpp"

unsigned int validateAnalysis(char* analysisBuffer, size_t analysisLength) {

    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysis;
    analysis.ParseFromString(analysisString);

    bool validity = true;
    if (!checkAllPathsPrivatized(analysis)) validity = false;

    // check that this function works
    toGraph(analysis);

    google::protobuf::ShutdownProtobufLibrary();
    return validity;
}

double computeEpsilon(char* analysisBuffer, size_t analysisLength) {

    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysis;
    analysis.ParseFromString(analysisString);

    // TODO: compute epsilon
    return 23.2;
}

char* generateReport(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength) {

    std::string reportString(R"({"message": "this is a release in the json schema format"})");
    return &reportString[0];
}