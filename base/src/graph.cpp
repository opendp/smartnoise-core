#include "../include/differential_privacy/graph.hpp"

#include "analysis.pb.h"
#include <iostream>

// Uncomment to force error if protobuf versions mismatch
//GOOGLE_PROTOBUF_VERIFY_VERSION;

signed int validate_analysis(char* analysisBuffer) {

    std::string analysisString(analysisBuffer);
    Analysis analysis;
    analysis.ParseFromString(analysisString);

    std::cout << analysis.DebugString();

    google::protobuf::ShutdownProtobufLibrary();
    return true;
}

void hello_world_validator () {
    std::cout << "Hello World!" << std::endl;
}
