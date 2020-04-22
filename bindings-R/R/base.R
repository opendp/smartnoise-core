# Title     : base
# Objective : core R data structures
# Created by: Consequences of Data
# Created on: 2020-04-20
# load proto descriptors
.onLoad <- function(libname, pkgname) {
  RProtoBuf::readProtoFiles2(
    files = c('api.proto', 'base.proto', 'components.proto', 'value.proto'),
    protoPath = file.path(system.file(package = "whitenoise"), "prototypes"))

  # list loaded messages
  # print(ls("RProtoBuf:DescriptorPool"))

  componentFilenames <- list.files(
    file.path(system.file(package = "whitenoise"), "prototypes", "components"),
    pattern = "*.json", full.names = TRUE)
  components <- lapply(componentFilenames, jsonlite::read_json)

  variantMessageMap <- lapply(components, function(component) component$name)
  names(variantMessageMap) <- lapply(components, function(component) component$id)
  assign("variantMessageMap", variantMessageMap, envir = parent.env(environment()))
}

# loadValidator <- function() {
#   dyn.load(file.path(getwd(), '../validator-c++/cmake-build-debug/lib/libdifferential_privacy.so'))
#   print('validator loaded')
#   .Call('hello_world_validator')
# }
