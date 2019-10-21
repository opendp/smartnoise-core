loadProtoDescriptors <- function() {
    protoFiles = c('release.proto', 'analysis.proto', 'types.proto')

    # working directory must be within ./prototypes for imports to work
    setwd('../../prototypes')
    RProtoBuf::readProtoFiles2(protoFiles)
    setwd('../bindings-R/R')

    # list loaded messages
    # print(ls("RProtoBuf:DescriptorPool"))
}

# loadValidator <- function() {
#   dyn.load(file.path(getwd(), '../validator-c++/cmake-build-debug/lib/libdifferential_privacy.so'))
#   print('validator loaded')
#   .Call('hello_world_validator')
# }

buildAnalysisExample <- function() {
    RProtoBuf::new(Analysis,
        graph=list(RProtoBuf::new(Analysis.GraphEntry,
            key=1,
            value=RProtoBuf::new(Component,
                constant=RProtoBuf::new(Constant, name="test")
            )
        ))
    )
}

validateAnalysis <- function(analysis) {
    message <- analysis$serialize(NULL)
    # analysisCopy <- Analysis$read(message)

    print(.Call('validate_analysis', message, package='dpBinding'))
}

#' Hello Rust!
#'
#' Examples of rust functions via C.
#'
#' @export
#' @rdname hellorust
#' @examples hello()
#' @useDynLib hellorust hello_wrapper
hello <- function() {
    .Call(hello_wrapper)
}
