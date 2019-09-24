#include "../include/differential_privacy/graph.hpp"

#include "analysis.pb.h"

// Uncomment to force error if protobuf versions mismatch
//GOOGLE_PROTOBUF_VERIFY_VERSION;

signed int validate(char* buffer) {

    std::string protoString(buffer);
    Analysis analysis;
    analysis.ParseFromString(protoString);

    std::cout << analysis.DebugString();

    google::protobuf::ShutdownProtobufLibrary();
    return true;
}