#include <catch2/catch.hpp>
#include "differential_privacy/graph.hpp"
#include "analysis.pb.h"

#include "differential_privacy_runtime_eigen/base.hpp"
#include "../../include/tests/main.hpp"

TEST_CASE("Mean", "[Statistics]") {
    Analysis* analysisProto = make_test_analysis();
    std::string analysisMessage = analysisProto->SerializeAsString();

    Release* releaseProto = make_test_release();
    std::string releaseMessage = releaseProto->SerializeAsString();

    int m = 10;
    int n = 3;

    auto** data = new double*[m];

    for (int i = 0; i < m; ++i) {
        data[i] = new double[n];
        for (int j = 0; j < n; ++j)
            data[i][j] = (double) i * j;
    }

    std::string colnames[] = {"col_A", "col_B", "col_C"};

    char** columns = new char*[n];
    for (int i = 0; i < n; ++i) columns[i] = const_cast<char *>(colnames[i].c_str());

    release(
            const_cast<char *>(analysisMessage.c_str()),
            const_cast<char *>(releaseMessage.c_str()),
            m, n,
            const_cast<const double**>(data),
            columns);
}
