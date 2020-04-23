
whitenoise::loadProtoDescriptors()

testCallAnalysis <- function() {
  analysis <- RProtoBuf::new(whitenoise.Analysis)
  release <- RProtoBuf::new(whitenoise.Release)
  whitenoise::validateAnalysis(analysis, release)
}

buildAnalysisExample <- function() {
    list(
        analysis=RProtoBuf::new(whitenoise.Analysis,
            computation_graph=RProtoBuf::new(whitenoise.ComputationGraph, value=list(RProtoBuf::new(whitenoise.ComputationGraph.ValueEntry(
                key=1,
                value=RProtoBuf::new(whitenoise.Component,
                    variant=RProtoBuf::new(whitenoise.Literal)
                )
            ))))
        ),
        release=RProtoBuf::new(whitenoise.Release, values=list(
            RProtoBuf::new(whitenoise.Release.ValuesEntry(
                key=1,
                value=RProtoBuf::new(whitenoise.ReleaseNode,
                    value=RProtoBuf::new(whitenoise.Value, ...), # TODO: Literal
                )
            ))
        ))
    )
}


test_that("build analysis", {
    buildAnalysisExample()
})

test_that("can release", {
    computeAnalysis(buildAnalysisExample())
})