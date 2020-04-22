#define R_NO_REMAP
#define STRICT_R_HEADERS
#include <R.h>
#include <Rinternals.h>

// Import C headers for rust API
#include "validator-rust/api.h"
#include "runtime-rust/api.h"

//SEXP raw_bytebuffer_test(SEXP buffer) {
//    // alloc vector of raw s-expressions
//
//    ByteBuffer response = struct ByteBuffer {
//        data: RAW(buffer),
//        len: length(buffer)
//    };
//    return Rf_ScalarString(Rf_mkCharLenCE(response.data, response.len, CE_UTF8));
//    PROTECT(response = Rf_allocVector(RAWSXP, length(buffer)));
//    UNPROTECT(1);
//    return response
//}

//roundtrip <- cfunction(c(buffer = "raw"), "
//
//    typedef struct {
//      int64_t len;
//      uint8_t *data;
//    } ByteBuffer;
//
//    ByteBuffer response {
//        .data= RAW(buffer),
//        .len= length(buffer)
//    };R
//
////    PROTECT(response_2 = Rf_allocVector(RAWSXP, response.len));
////    PROTECT(response_2 = Rf_coerceVector(response.data, RAWSXP)))
////    UNPROTECT(1);
////    return Rf_coerceVector(*response.data, RAWSXP);
//    return Rf_ScalarString(Rf_mkCharLenCE((const char *)(response.data), response.len, CE_UTF8));
//")


// Actual Wrappers
SEXP validate_analysis_wrapper(SEXP buffer) {
    ByteBufferValidator response = validate_analysis(RAW(buffer), Rf_length(buffer));
//    return Rf_ScalarString(Rf_mkCharLenCE(response.data, response.len, CE_UTF8));
    return Rf_ScalarString(Rf_mkCharLenCE((const char *)(response.data), response.len, CE_UTF8));
//    return Rf_ScalarString(Rf_mkCharCE(validate_analysis(), CE_UTF8));
}

//SEXP compute_release_wrapper() {
//    return Rf_ScalarString(Rf_mkCharCE(compute_release(), CE_UTF8));
//}

// Standard R package stuff
static const R_CallMethodDef CallEntries[] = {
        {"validate_analysis_wrapper", (DL_FUNC) &validate_analysis_wrapper, 1},
//        {"compute_release", (DL_FUNC) &compute_release_wrapper, 0},
        {NULL, NULL, 0}
};

void R_init_whitenoise(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
