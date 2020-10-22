Contributing to SmartNoise
=============================
Contributions to SmartNoise are welcome from all members of the community. This document is here to simplify the 
on-boarding experience for contributors, contributions to this document are also welcome. 
Please use the [GitHub issue board](https://github.com/orgs/opendifferentialprivacy/projects/1) to track and take ownership of issues. 

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/smartnoise-core/issues). We appreciate all contributions. We welcome pull requests with bug-fixes without prior discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and 
discuss the feature with us. Sending a PR without discussion might end up resulting in a rejected PR, because we might
 be taking the core in a different direction than you might be aware of.

For a description of the library architecture and installation instructions, see [smartnoise-core/README.md](./README.md). 
Before contributing, we recommend following the [Getting Started guide](./README.md#getting-started) in that README to get sample notebooks running and to gain some familiarity with the library structure.

General Contribution Guidelines
===============================

- Use [GitHub issues](https://github.com/opendifferentialprivacy/smartnoise-core/issues) including the [Github issue board](https://github.com/orgs/opendifferentialprivacy/projects/1).
- Anything implemented from the differential privacy literature should contain clear citations and/or whitepapers 
explaining any necessary derivations.
- Code should be clearly documented and include testing. For more information, please see:
  - Block tests in Rust: https://doc.rust-lang.org/rustdoc/documentation-tests.html
  - Testing modules in Rust: https://doc.rust-lang.org/book/ch11-03-test-organization.html 

Contributing to the Validator
=============================

Components need to be added to both the protobuf and to the source codebase. 

### 1. Add a component to protobuf in validator-rust/prototypes:

Add a JSON file to the [components subdirectory](validator-rust/prototypes/components). An entry in components.proto will be made upon build. 

### 2. Add component to src/components:

This will look different depending on the type of component you are contributing. The following list describes traits
that must be implemented for common types of components. 

### For new statistics:
  - Include a `Component` implementation containing the `propagate_property` function which 
        how to propagate properties through that node in the graph. This function also 
        checks that differential privacy guarantees are met: e.g. that the sensitivity is computable,
        that the data is conformable, and/or overflows can cause the computation to fail.
  - A `Sensitivity` implementation with a `compute_sensitivity` function that describes how to compute
        the statistic for all combinations of implemented privacy definitions and sensitivity spaces.
        - Any derivations used for sensitivities should be derived in the [`whitepapers/sensitivities`](whitepapers/sensitivities),
        or there should be a citation to published sensitivity analysis (or both).
        - Include a comment in the `Sensitivity` implementation that links to the location of the proof

### For new DP statistics:
  - An `Expandable` implementation with an `expand_component` function which describes how to expand the computation graph
        to insert each component of the differentially private calculation.
  - A `Report` implementation with a `summarize` function that stores the results of the differentially private computation 
        as JSON.

#For new mechanisms:
 -  A `Component` implementation containing a `propagate_property` function describing 
    how to propagate input properties through that node in the graph. This also contains checks on the 
    privacy parameters to verify that they are reasonable.
 - An `Expandable` implementation with an `expand_component` function which describes how to expand the computation graph
 - If possible, an `Accuracy` implementation with `accuracy_to_privacy_usage` and `privacy_usage_to_accuracy`
    functions that describe how to transition between accuracy and privacy guarantees. Any associated derivations 
    should be recorded in [`whitepapers/accuracy`](whitepapers/accuracies).
 - Contributors should add a whitepaper on the mechanism itself in  [`whitepapers/mechanisms`](whitepapers/mechanisms) or 
      clearly cite the academic paper the mechanism originated from.
   
Contributing to the Rust Runtime
================================

Each [Component](runtime-rust/src/components) in the Rust runtime (`runtime-rust`) requires an `Evaluable` trait that describes how to evaluate a node of the computation graph. Any new component should include documentation for both the arguments to the `evaluate` function and the the return type(s). Additionally, doctests and test modules should be included.

In order to contribute to underlying mechanisms and noise selection, which are implemented in the crate's [utility 
functions](https://github.com/opendifferentialprivacy/smartnoise-core/tree/develop/runtime-rust/src/utilities), first read the noise whitepaper in [`whitepaper/noise`](whitepapers/noise) to understand the current design choices. Clearly document and test any code.

Documentation and Testing
=======================
Code contributions should include both doctests and unit tests. We also encourage contributors to run the samples in 
[smartnoise-samples](https://github.com/opendifferentialprivacy/smartnoise-samples) and to write their own samples that highlight their contributions. All code is integration tested and reviewed before merging. 
