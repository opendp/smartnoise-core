#ifndef DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_API_HPP
#define DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_API_HPP

#include <release.pb.h>
#include <analysis.pb.h>

extern "C" {

    char* release(
            char* analysisBuffer, size_t analysisLength,
            char* releaseBuffer, size_t releaseLength,
            char* dataPath, size_t dataPathLength,
            char* header, size_t headerLength);

    // for deallocating pointers to malloc'ed char arrays
    void free_ptr(char* ptr);
}


#endif //DIFFERENTIAL_PRIVACY_RUNTIME_EIGEN_API_HPP
