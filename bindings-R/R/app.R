loadProtoDescriptors <- function() {
    protoFiles = c('release.proto', 'analysis.proto', 'types.proto', 'dataset.proto')

    # working directory must be within ./prototypes for imports to work
    setwd('../src/prototypes')
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

    print(.Call('validate_analysis', message, package='burdock'))
}

#' Hello Rust!
#'
#' Examples of rust functions via C.
#'
#' @export
#' @rdname burdock
#' @examples hello()
#' @useDynLib burdock hello_wrapper
hello <- function() {
    .Call(hello_wrapper)
}
