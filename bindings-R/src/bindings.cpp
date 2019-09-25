#include <iostream>
#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
void validate_analysis(SEXP message) {
    // TODO: link validator .so
}

// [[Rcpp::export]]
void hello() {
    std::cout << "Hello World!" << std::endl;
}
