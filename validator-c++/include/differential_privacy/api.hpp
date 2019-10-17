#ifndef DIFFERENTIAL_PRIVACY_API_HPP
#define DIFFERENTIAL_PRIVACY_API_HPP

#include <cstddef>
#include <analysis.pb.h>

extern "C" {
    unsigned int validate_analysis(char* analysisBuffer, size_t analysisLength);
    double compute_epsilon(char* analysisBuffer, size_t analysisLength);
    char* generate_report(char* analysisBuffer, size_t analysisLength, char* releaseBuffer, size_t releaseLength);

    // for deallocating pointers to malloc'ed char arrays
    void free_ptr(char* ptr);
}


#endif //DIFFERENTIAL_PRIVACY_API_HPP
