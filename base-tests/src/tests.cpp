#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>
#include <differential_privacy/pipelines.hpp>
#include "differential_privacy/base.hpp"

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

    Datasource dataset = Datasource("dataset");
}

TEST_CASE("Analysis_epsilon", "[Analysis]") {
    Datasource datasource = Datasource("dataset");
    Analysis analysis = Analysis();
    Laplace mean = DPMean(datasource, std::list<double>({0., 1.}));
    analysis.add(mean);
//    std::cout << "Epsilon: " << analysis.get_epsilon();
}