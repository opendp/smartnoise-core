#include "../include/differential_privacy_runtime_eigen/base.hpp"
#include "../../base/include/differential_privacy/mechanisms.hpp"

#include <map>
#include <Eigen/src/Core/Matrix.h>
#include <openssl/rand.h>


Eigen::MatrixXi release(Component component, Eigen::MatrixXi data) {
    return forward(component, data);
}

// release with a vector of data
// only applies if one column of one table is used in the analysis
void release(Component* component, std::vector<double> data) {
    auto* sources = component->get_sources();
    if (sources->size() > 1) throw "Release requires multiple labeled sources.";
    std::string dataset = sources->front().first;
    std::string column = sources->front().second;

    std::map<std::string, std::map<std::string, std::vector<double>>> wrapped = {{dataset, {{column, data}}}};
//    release(component, wrapped);
}

// need to return both release object and data
std::pair<std::list<Release<double>>, DataSet> release(Laplace* component, DataSet data) {
    for (auto* child : component->get_children()) {
        for (auto* values : release(child, data))
    }
    return
}

DataSet propagate(Laplace* component, DataSet data) {
    RAND_poll();
    double buffer[] = nullptr;
    RAND_bytes(buffer, 64);

    float sample = static_cast<float>(buffer) / static_cast<float>(RAND_MAX);

}