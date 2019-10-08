
#include <openssl/rand.h>
#include <openssl/err.h>
#include <iostream>
#include <cstring>
#include <cmath>
#include "../include/differential_privacy_runtime_eigen/utilities.hpp"

// TODO: vectorize?
double sampleLaplace(double mu, double scale) {
    double uniformSample = sampleUniform();
    if (uniformSample < mu)
        return .5 * exp(abs(uniformSample - mu) / scale);
    return 1 - .5 * exp(-abs(uniformSample - mu) / scale);
}

double sampleUniform(double low, double high) {
    if (high < low) std::swap(low, high);

    unsigned char buffer[sizeof(double)];

    int rc = RAND_bytes(buffer, sizeof(buffer));
    unsigned long err = ERR_get_error();

    // TODO: remove cout from library code
    if (rc != 1)
        std::cout << "OpenSSL failed with error code: " << err << std::endl;

    double uniformSample;
    memcpy(&uniformSample, buffer, sizeof(buffer));

    return fmod(uniformSample, high - low) + low;
}