#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <analysis.pb.h>
#include <release.pb.h>

Analysis* make_test_analysis() {
    auto* analysis = new Analysis();
    auto* constant = new Constant();

    auto* component = new Component();
    component->set_allocated_constant(constant);

    auto& graph = *analysis->mutable_graph();
    graph[23] = *component;

    return analysis;
}

Release* make_test_release() {
    return new Release();
}