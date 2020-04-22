# Title     : api helpers
# Objective : Helper R functions for calling to the C library
# Created by: Consequences of Data
# Created on: 2020-04-20

#' Validate analysis
#'
#' @export
#' @rdname whitenoise
#' @useDynLib whitenoise validate_analysis_wrapper
validateAnalysis <- function(analysis, release) {
  request <- RProtoBuf::new(whitenoise.RequestValidateAnalysis,
                            analysis = analysis,
                            release = release)
  print(c("request", request))

  request_message <- request$serialize(NULL)
  print(c("request_message", request_message))

  response_message <- .Call(validate_analysis_wrapper, request_message, PACKAGE = 'whitenoise')
  print(c("response_message", response_message))

  response <- whitenoise.ResponseValidateAnalysis$read(response_message)
  print(c("response", response))

  response
}

# #' Compute release
# #'
# #' @export
# #' @rdname whitenoise
# #' @examples computeRelease(buildAnalysisExample())
# #' @useDynLib whitenoise compute_release
# computeRelease <- function(analysis, release, stack_traces, filter_level) {
#
#   request <- RProtoBuf::new(whitenoise.RequestRelease,
#                             analysis = analysis,
#                             release = release,
#                             stack_traces = stack_traces,
#                             filter_level = filter_level
#   )
#   print(c("request", request))
#
#   request_message <- request$serialize(NULL)
#   print(c("request_message", request_message))
#
#   response_message <- .Call('compute_release', request_message, package = 'whitenoise')
#   print(c("response_message", response_message))
#
#   response <- whitenoise.ResponseRelease$read(response_message)
#   print(c("response", response))
#
#   response
# }
#
