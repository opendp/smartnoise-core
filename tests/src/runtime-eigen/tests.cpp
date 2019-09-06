#include <catch2/catch.hpp>
#include "differential_privacy/pipelines.hpp"
#include "differential_privacy/components.hpp"

#include "differential_privacy_runtime_eigen/base.hpp"

TEST_CASE("Mean", "[Statistics]") {
    Datasource datasource = Datasource("dataset_1", "column_1");
    Component mean = (Component) DPMean(datasource, std::list<double>({0., 1.}));

    std::map<std::string, std::map<std::string, std::vector<double>>> data = {
            {"dataset_1", {
                {"column_1", {1.2, 2.3, 3.4, 4.5, 5.6}}}}};

//    release(mean, data);
//    std::cout << "Epsilon: " << analysis.get_epsilon();
}
