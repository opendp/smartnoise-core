#include <catch2/catch.hpp>
#include <differential_privacy/graph.hpp>
#include "analysis.pb.h"

#include <iostream>

TEST_CASE("Validate_1", "[Validate]") {
    auto* analysis = new Analysis();
    auto* constant = new Constant();

    constant->set_id(23);
    Component* graph = analysis->add_graph();
    graph->set_allocated_constant(constant);

    std::string message = analysis->SerializeAsString();
    assert(validate(const_cast<char *>(message.c_str())));
}
