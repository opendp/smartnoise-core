#include <catch2/catch.hpp>
#include <differential_privacy/graph.hpp>
#include "analysis.pb.h"

#include <iostream>

TEST_CASE("Validate_1", "[Validate]") {
    auto* analysis = new Analysis();
    auto* constant = new Constant();

    auto* component = new Component();
    component->set_allocated_constant(constant);

    auto& graph = *analysis->mutable_graph();
    graph[23] = *component;

    std::string message = analysis->SerializeAsString();
    assert(validate_analysis(const_cast<char *>(message.c_str())));
}
