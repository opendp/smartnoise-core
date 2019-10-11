#include <R.h>
#include <Rinternals.h>
#include "libreadwrite.h"

SEXP Writeit(SEXP data, SEXP path, SEXP length) {
    char *arg1 = (char *) 0;
    char *arg2 = (char *) 0;
    int arg3 = 0;
    arg1 = (char *)(strdup(CHAR(STRING_ELT(data, 0))));
    arg2 = (char *)(strdup(CHAR(STRING_ELT(path, 0))));
    arg3 = INTEGER(length)[0];
    Write(arg1, arg2, arg3);
    free(arg1);
    free(arg2);

    return R_NilValue;
}

SEXP Readit(SEXP path){
    SEXP r_ans = R_NilValue ;
    char *arg1 = (char *) 0;
    char *result = 0 ;
    arg1 = (char *)(strdup(CHAR(STRING_ELT(path, 0))));
    result = (char *)Read(arg1) + 8;
    free(arg1);

    r_ans = result ? Rf_mkString((char *)(result)) : R_NilValue;

    return r_ans;
}
