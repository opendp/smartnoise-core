library(burdock)
loadProtoDescriptors()


test_that("build analysis", {
    buildAnalysisExample()
})

test_that("can validate", {
    validateAnalysis(buildAnalysisExample())
})

test_that("bindings accessible" {
    .Call('hello')
})
