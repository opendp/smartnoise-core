#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <analysis.pb.h>
#include <release.pb.h>

burdock::Analysis* make_test_analysis() {
    auto* analysis = new burdock::Analysis();
    auto* constant = new burdock::Constant();

    auto* component = new burdock::Component();
    component->set_allocated_constant(constant);

    auto& graph = *analysis->mutable_graph();
    graph[23] = *component;

    return analysis;
}

burdock::Release* make_test_release() {
    return new burdock::Release();
}