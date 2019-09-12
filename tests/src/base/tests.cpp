#include <catch2/catch.hpp>
#include <differential_privacy/pipelines.hpp>
#include <differential_privacy/aggregations.hpp>
#include "differential_privacy/components.hpp"

#include <iostream>

TEST_CASE("Node_1", "[Component]") {
    Component node = Component();
    assert(!node.get_will_release());
}

TEST_CASE("PrivacyDefinition_1", "[PrivacyDefinition]") {
    PrivacyDefinition definition = PrivacyDefinition();
}

TEST_CASE("Analysis_graph", "[Analysis]") {
    Analysis analysis = Analysis();
    std::string input_tag = "dataset";

    auto* datasource = new Datasource("dataset_1", "column_1");
    auto* test = new Mean(datasource);
    std::cout << "type" << std::endl << test->get_name();
}

TEST_CASE("Analysis_epsilon", "[Analysis]") {
    auto* datasource = new Datasource("dataset_1", "column_1");
    auto* analysis = new Analysis();
    auto* mean = DPMean(datasource, std::list<double>({0., 1.}));
    analysis->add(mean);
//    std::cout << "Epsilon: " << analysis.get_epsilon();
}