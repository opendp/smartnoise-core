# Title     : value buffering
# Objective : serialization/deserialization between R and proto objects
# Created by: Consequences of Data
# Created on: 2020-04-20

#' @export
serializePrivacyUsage <- function(usage) {
  if (is.na(usage)) {
    return(list())
  }
}
