[**Please note that we have recently renamed this system.**](https://projects.iq.harvard.edu/opendp/blog/building-inclusive-community)

---

[![Build Status](https://travis-ci.com/opendifferentialprivacy/smartnoise-core.svg?branch=develop)](https://travis-ci.com/opendifferentialprivacy/smartnoise-core)


<a href="http://opendp.io"><img src="images/SmartNoise Logo/SVG/LogoMark_color.svg" align="left" height="70" vspace="8" hspace="18"></a>


## <br/>SmartNoise Core Differential Privacy Library <br/>

See also the accompanying [SmartNoise System repository](https://github.com/opendifferentialprivacy/smartnoise-system) and [SmartNoise Samples repository](https://github.com/opendifferentialprivacy/smartnoise-samples) accompanying repositories for this system.

---

Differential privacy is the gold standard definition of privacy protection.  The SmartNoise project, in collaboration with OpenDP, aims to connect theoretical solutions from the academic community with the practical lessons learned from real-world deployments, to make differential privacy broadly accessible to future deployments.  Specifically, we provide several basic building blocks that can be used by people involved with sensitive data, with implementations based on vetted and mature differential privacy research.  Here in the Core, we provide a pluggable open source library of differentially private algorithms and mechanisms for releasing privacy preserving queries and statistics, as well as APIs for defining an analysis and a validator for evaluating these analyses and composing the total privacy loss on a dataset.

The mechanisms library provides a fast, memory-safe native runtime for validating and running differentially private analyses.  The runtime and validator are built in Rust, while Python support is available and R support is forthcoming.

Differentially private computations are specified as an analysis graph that can be validated and executed to produce differentially private releases of data.  Releases include metadata about accuracy of outputs and the complete privacy cost of the analysis.


- [More about SmartNoise Core](#more-about-smartnoise-core)
  - [Components](#components)
  - [Architecture](#architecture)
- [Installation](#installation)
  - [Crates.io](#cratesio)
  - [From Source](#from-source)
- [Getting Started](#getting-started)
  - [Jupyter Notebook Examples](#jupyter-notebook-examples)
  - [SmartNoise Core Rust Documentation](#smartnoise-core-rust-documentation)
- [Communication](#communication)
- [Releases and Contributing](#releases-and-contributing)
- [Contributing Team](#contributing-team)

---

## More about SmartNoise Core

### Components

The primary releases available in the library, and the mechanisms for generating these releases, are enumerated below.
For a full listing of the extensive set of components available in the library [see this documentation.](https://opendifferentialprivacy.github.io/smartnoise-core/doc/smartnoise_validator/docs/components/index.html)

| Statistics    | Mechanisms | Utilities  |
| ------------- |------------|------------|
| Count         | Gaussian   | Cast       |
| Histogram     | Geometric  | Clamping   |
| Mean          | Laplace    | Digitize   |
| Quantiles     |            | Filter     |
| Sum           |            | Imputation |
| Variance/Covariance |      | Transform  |


<!-- (forthcoming: links to minisite/research papers) -->

### Architecture

There are three sub-projects that address individual architectural concerns. These sub-projects communicate via protobuf messages that encode a graph description of an arbitrary computation, called an `analysis`.

#### 1. Validator
  - Location: `/validator-rust`

The core library, is the `validator`, which provides a suite of utilities for checking and deriving sufficient conditions for an analysis to be differentially private. This includes checking if specific properties have been met for each component, deriving sensitivities, noise scales and accuracies for various definitions of privacy, building reports and dynamically validating individual components. This library is written in Rust.

#### 2. Runtime
  - Location: `/runtime-rust`

There must also be a medium to execute the analysis, called a `runtime`. There is a reference runtime written in Rust, but runtimes may be written using any computation framework--be it SQL, Spark or Dask--to address your individual data needs.

#### 3. Bindings
  - Python Bindings: [core-python](https://github.com/opendifferentialprivacy/smartnoise-core-python)
  - R Bindings (in progress): [core-R](https://github.com/opendifferentialprivacy/smartnoise-core-R)
  - Rust Bindings (in progress): [core-Rust](https://opendifferentialprivacy.github.io/smartnoise-core/doc/smartnoise_validator/bindings/index.html)

Finally, there are helper libraries for building analyses, called `bindings`. Bindings may be written for any language, and are thin wrappers over the validator and/or runtime(s). Language bindings are currently available for Python, with support for at minimum R, Rust and SQL forthcoming.


#### Note on Protocol Buffers

  - Location: `/validator-rust/prototypes`

Communication among projects is handled via [Protocol Buffer definitions](https://developers.google.com/protocol-buffers/) in the `/validator-rust/prototypes` directory. All three sub-projects implement:
  - Protobuf code generation
  - Protobuf serialization/deserialization
  - Communication over [FFI](https://en.wikipedia.org/wiki/Foreign_function_interface)
  - Handling of distributable packaging

At some point the projects have compiled cross-platform (more testing needed). The validator and reference runtime compile to standalone libraries that may be linked into your project, allowing communication over C foreign function interfaces.

## Installation
Refer to [troubleshooting.md](troubleshooting.md) for install problems.

#### PyPi packages
Refer to [core-python](https://github.com/opendifferentialprivacy/smartnoise-core-python) which contains python bindings, including links to PyPi packages.

#### Crates.io
The crates are intended for library consumers.

The Rust Validator and Runtime are available as crates:
- Validator: [validator](https://crates.io/crates/smartnoise_validator) on crates.io
- Runtime: [runtime](https://crates.io/crates/smartnoise_runtime) on crates.io

### From Source
The source install is intended for library developers.

You may find it easier to use the library with this repository set up as a submodule of some set of language bindings. 
In this case, switch to the language bindings setup.
You can still push commits and branches from the core submodule of whatever bindings language you prefer.
- [Python](https://github.com/opendifferentialprivacy/smartnoise-core-python#from-source)
- [R (WIP)](https://github.com/opendifferentialprivacy/smartnoise-core-R#installation)
- [Rust (WIP)](https://opendifferentialprivacy.github.io/smartnoise-core/doc/smartnoise_validator/bindings/index.html)

1. Clone the repository
    
        git clone git@github.com:opendifferentialprivacy/smartnoise-core.git

2. Install system dependencies (rust, gcc)   
    Mac:
    ```shell script
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    xcode-select --install
    ```
    
    Linux:
    ```shell script
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    sudo apt-get install diffutils gcc make m4
    ```

    Windows:
    Install WSL and refer to the linux instructions.

3. In a new terminal:  
    Build crate
    
        cargo build
    
    Test crate
    
        cargo test
    
    Document crate
    
        cargo rustdoc --open
    
    Build production docs
    
        ./build_docs.sh
    
There are crates in `validator-rust` and `runtime-rust`, and a virtual crate in root that runs commands on both.
Switch between crates via `cd`, or by setting the manifest path `--manifest-path=validator-rust/Cargo.toml`.


---
## Getting Started

### Jupyter Notebook Examples

We have [numerous Jupyter notebooks](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis) demonstrating the use of the Core library and validator through our Python bindings.  These are in our accompanying [samples repository](https://github.com/opendifferentialprivacy/smartnoise-samples) which has exemplars, notebooks and sample code demonstrating most facets of this project.

[<img src="images/figs/plugin_mean_comparison.png" alt="Relative error distributions" height="100">](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis)
[<img src="images/figs/example_size.png" alt="Release box plots" height="100">](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis)
[<img src="images/figs/example_education.png" alt="Histogram releases" height="100">](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis)
[<img src="images/figs/example_utility.png" alt="Utility simulations" height="100">](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis)
[<img src="images/figs/example_simulations.png" alt="Bias simulations" height="100">](https://github.com/opendifferentialprivacy/smartnoise-samples/tree/master/analysis)

### SmartNoise Core Rust Documentation

The [Rust documentation](https://opendifferentialprivacy.github.io/smartnoise-core/) includes full documentation on all pieces of the library and validator, including extensive [component by component descriptions with examples](https://opendifferentialprivacy.github.io/smartnoise-core/doc/smartnoise_runtime/components/index.html).

## Communication

- Please use [GitHub issues](https://github.com/opendifferentialprivacy/smartnoise-core/issues) for bug reports, feature requests, install issues, and ideas.
- [Gitter](https://gitter.im/opendifferentialprivacy/SmartNoise) is available for general chat and online discussions.
- For other requests, please contact us at [smartnoise@opendp.io](mailto:smartnoise@opendp.io).
  - _Note: We encourage you to use [GitHub issues](https://github.com/opendifferentialprivacy/smartnoise-core/issues), especially for bugs._

## Releases and Contributing

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/smartnoise-core/issues).

We appreciate all contributions. We welcome pull requests with bug-fixes without prior discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and discuss the feature with us.
  - Sending a PR without discussion might end up resulting in a rejected PR, because we may be taking the core in a different direction than you might be aware of.

## Contributing Team

Joshua Allen, Christian Covington, Eduardo de Leon, Ira Globus-Harris, James Honaker, Jason Huang, Saniya Movahed, Michael Phelan, Raman Prasad, Michael Shoemate, You?
