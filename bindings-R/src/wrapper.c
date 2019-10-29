#define R_NO_REMAP
#define STRICT_R_HEADERS
#include <Rinternals.h>

// Import C headers for rust API
#include "runtime-rust/api.h"

// Actual Wrappers
SEXP hello_wrapper() {
    return Rf_ScalarString(Rf_mkCharCE(string_from_rust(), CE_UTF8));
}

// Standard R package stuff
static const R_CallMethodDef CallEntries[] = {
        {"hello_wrapper", (DL_FUNC) &hello_wrapper, 0},
        {NULL, NULL, 0}
};

void R_init_burdock(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}