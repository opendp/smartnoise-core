#ifndef DIFFERENTIAL_PRIVACY_API_HPP
#define DIFFERENTIAL_PRIVACY_API_HPP

#include <cstddef>
#include <analysis.pb.h>

extern "C" {
    unsigned int validateAnalysis(char* analysisBuffer, size_t analysisLength);
    double computeEpsilon(char* analysisBuffer, size_t analysisLength);
    char* generateReport(char* analysisBuffer, size_t analysisLength, char* releaseBuffer, size_t releaseLength);

    // for deallocating pointers to malloc'ed char arrays
    void freePtr(char* ptr);
}


#endif //DIFFERENTIAL_PRIVACY_API_HPP
