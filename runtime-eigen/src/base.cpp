#include "../include/differential_privacy_runtime_eigen/base.hpp"

#include <map>
//#include <Eigen/src/Core/Matrix.h>


//Eigen::MatrixXi release(Component component, Eigen::MatrixXi data) {
//    return forward(component, data);
//}

// release with a vector of data
// only applies if one column of one table is used in the analysis
void release(Component component, std::vector<double> data) {
    auto sources = component.get_sources();
    if (sources->size() > 1) throw "Release requires multiple labeled sources.";
    std::string dataset = sources->front().first;
    std::string column = sources->front().second;

    std::map<std::string, std::map<std::string, std::vector<double>>> wrapped = {{dataset, {{column, data}}}};
//    release(component, wrapped);
}
