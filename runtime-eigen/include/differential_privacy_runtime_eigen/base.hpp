
#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP


#include <release.pb.h>
#include <analysis.pb.h>

#include <Eigen/Dense>


extern "C" {
    int release(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength,
        int m, int n, const double** data, char** columns,
        char* responseBuffer, size_t responseLength);
}

Release execute(Analysis analysis, Release release, Eigen::MatrixXd data, std::vector<std::string> columns);

#endif //DIFFERENTIAL_PRIVACY_BASE_HPP
