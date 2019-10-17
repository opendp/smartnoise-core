#include "analysis.pb.h"
#include <iostream>

#include "../include/differential_privacy/api.hpp"
#include "../include/differential_privacy/base.hpp"
#include "../include/differential_privacy/backtrace.hpp"

unsigned int validate_analysis(char* analysisBuffer, size_t analysisLength) {

#if defined(BACKTRACE_MODE)
    struct sigaction sigact;

    sigact.sa_sigaction = crit_err_hdlr;
    sigact.sa_flags = SA_RESTART | SA_SIGINFO;

    if (sigaction(SIGSEGV, &sigact, (struct sigaction *) NULL) != 0) {
        fprintf(stderr, "error setting signal handler for %d (%s)\n",
                SIGSEGV, strsignal(SIGSEGV));

        exit(EXIT_FAILURE);
    }
#endif

    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysis;
    analysis.ParseFromString(analysisString);

    bool validity = true;
    if (!checkAllPathsPrivatized(analysis)) validity = false;

    toGraph(analysis);

//    calling this is tricky with dll files
//    google::protobuf::ShutdownProtobufLibrary();
    return validity;
}

double compute_epsilon(char* analysisBuffer, size_t analysisLength) {

#if defined(BACKTRACE_MODE)
    struct sigaction sigact;

    sigact.sa_sigaction = crit_err_hdlr;
    sigact.sa_flags = SA_RESTART | SA_SIGINFO;

    if (sigaction(SIGSEGV, &sigact, (struct sigaction *) NULL) != 0) {
        fprintf(stderr, "error setting signal handler for %d (%s)\n",
                SIGSEGV, strsignal(SIGSEGV));

        exit(EXIT_FAILURE);
    }
#endif

    std::string analysisString(analysisBuffer, analysisLength);
    burdock::Analysis analysis;
    analysis.ParseFromString(analysisString);

    double total = 0;
    for (const auto& componentPair : analysis.graph()) {
        burdock::Component component = componentPair.second;
        if (component.has_dpmeanlaplace()) {
            total += component.dpmeanlaplace().epsilon();
        }
    }
    return total;
}

char* generate_report(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength) {

#if defined(BACKTRACE_MODE)
    struct sigaction sigact;

    sigact.sa_sigaction = crit_err_hdlr;
    sigact.sa_flags = SA_RESTART | SA_SIGINFO;

    if (sigaction(SIGSEGV, &sigact, (struct sigaction *) NULL) != 0) {
        fprintf(stderr, "error setting signal handler for %d (%s)\n",
                SIGSEGV, strsignal(SIGSEGV));

        exit(EXIT_FAILURE);
    }
#endif

    const char *reportString(R"({"message": "this is a release in the json schema format"})");

    // invokes malloc for a string duplicate to preserve memory after this stack frame popped
    return strdup(reportString);
}

void free_ptr(char* ptr) {
    free(ptr);
}