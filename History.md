### v0.1.2
* Minor readme changes

### v0.1.1
* Python Bindings: enable `protect_floating_point` by default. 
    - Real-valued queries are less susceptible to floating-point attacks, at the cost of utility 
    - Use `sn.Analysis(protect_floating_point=False)` to enable the laplace and (analytic) gaussian mechanisms 
* Fix noise scaling issues in the Gaussian and Analytic Gaussian mechanism
* Fixes for Gaussian and Analytic Gaussian accuracy
* Postprocess geometric mechanism noise with clamping
* Compute sensitivities as integers whenever possible (counts, histograms, sums)
* Added runtime sanity checks to detect violations of static properties in pre-aggregated data
* O(n^2) -> O(n) runtime performance in exponential mechanism and categorical imputation
* Fixed an incorrect inference of dataset size when transforming a dataset with unknown size against a broadcastable scalar
* Unions always permitted on public data
* Added inference of nature (categories, bounds) to ToInt
* Plug-in mean derives bounds for sum in laplace and geometric mechanism

### v0.1
* Renamed package to Smartnoise, version number reset
* Added snapping mechanism
* Added analytic gaussian mechanism
* Added DP Linear Regression through the Theil-Sen transform and gumbel mechanism
* Added generalized resize for privacy amplification by subsampling
* Tightened c-stability checks to protect against adversarial dataset reordering when unioning data partitions
* Modified error messages to contain suggested fixes
* Bugfix to retain statistics when generating reports
