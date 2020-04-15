[![Build Status](https://travis-ci.org/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.org/opendifferentialprivacy/whitenoise-core)

<a href="http://opendp.io"><img src="images/WhiteNoise Logo/SVG/Full_color.svg" align="left" height="70" vspace="8" hspace="18"></a>

## WhiteNoise Core <br/> Differential Privacy Library <br/>

See also the accompanying [WhiteNoise-System](https://github.com/opendifferentialprivacy/whitenoise-system) and [WhiteNoise-Samples](https://github.com/opendifferentialprivacy/whitenoise-samples) repositories for this system.

##

Differential privacy is the gold standard definition of privacy protection.  The WhiteNoise project aims to connect theoretical solutions from the academic community with the practical lessons learned from real-world deployments, to make differential privacy broadly accessible to future deployments.  Specifically, we provide several basic building blocks that can be used by people involved with sensitive data, with implementations based on vetted and mature differential privacy research.  In WhiteNoise Core, we provide a pluggable open source library of differentially private algorithms and mechanisms for releasing privacy preserving queries and statistics, as well as APIs for defining an analysis and a validator for evaluating these analyses and composing the total privacy loss on a dataset. 

The mechanisms library provides a fast, memory-safe native runtime for validating and running differentially private analyses.  The runtime and validator are built in Rust, while Python support is available and R support is forthcoming.

Differentially private computations are specified as an analysis graph that can be validated and executed to produce differentially private releases of data.  Releases include metadata about accuracy of outputs and the complete privacy cost of the analysis.


- [More about WhiteNoise Core](#more-about-whitenoise-core)
  - [Component List](#components)
  - [Architecture](#architecture)
- [Installation](#installation)
  - [Binaries](#binaries)
  - [From Source](#from-source)
- [Getting Started](#getting-started)
  - [Jupyter Notebook Examples](#jupyter-notebook-examples)
  - [WhiteNoise Rust Documentation](#whitenoise-rust-documentation)
- [Communication](#communication)
- [Releases and Contributing](#releases-and-contributing)
- [Contributing Team](#contributing-team)

---

## More about WhiteNoise Core

### Components

The primary releases available in the library, and the mechanisms for generating these releases, are enumerated below.
For a full listing of the extensive set of components available in the library [see this documentation.](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_validator/docs/components/index.html)

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

The core library is the `validator`, which provides a suite of utilities for checking and deriving necessary conditions for an analysis to be differentially private. This includes checking if sufficient properties have been met for each component, deriving sensitivities, noise scales and accuracies for various definitions of privacy, building reports and dynamically validating individual components. This library is written in rust.

There must also be a medium to execute the analysis, called a `runtime`. There is a reference runtime written in rust, but runtimes may be written using any computation framework- be it SQL, Spark or Dask- to address your individual data needs. 

Finally, there are helper libraries for building analyses, called `bindings`. Bindings may be written for any language, and are thin wrappers over the validator and/or runtime(s). Language bindings are currently available for Python, with support for at minimum R and SQL forthcoming.

All projects implement protobuf code generation, protobuf serialization/deserialization, communication over FFI, handle distributable packaging, and have at some point compiled cross-platform (more testing needed). Communication among projects is handled via proto definitions from the `prototypes` directory. The validator and reference runtime compile to standalone libraries that may be linked into your project, allowing communication over C foreign function interfaces.

## Installation

### Binaries

- (forthcoming PyPi binaries via milksnake)

### From Source

1. Clone the repository

        git clone $REPOSITORY_URI

2. Install Rust

    Mac, Linux:

        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    Close terminal and open new terminal to add cargo to path.
    You can test with `rustc --version`

3. Install protobuf compiler
    Mac:

        brew install protobuf

    Ubuntu:

        sudo snap install protobuf --classic

    Windows:

        choco install protoc

  * For non-Chocolatey users: download and install the latest build
    + https://github.com/protocolbuffers/protobuf/releases/latest


4. Install the python bindings

        cd bindings-python
        pip install -e ".[test,plotting]"

    If you are doing package development, I recommend using `bindings-python/debug_*.sh` for debugging.

---
## Getting Started

### Jupyter Notebook Examples

We have [numerous Jupyter notebooks](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis) demonstrating the use of the WhiteNoise library and validator through our Python bindings.  These are in our accompanying [WhiteNoise-Samples repository](https://github.com/opendifferentialprivacy/whitenoise-samples) which has exemplars, notebooks and sample code demonstrating most facets of this project.

[<img src="images/figs/plugin_mean_comparison.png" alt="Relative error distributions" height="100">](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis)
[<img src="images/figs/example_size.png" alt="Release box plots" height="100">](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis)
[<img src="images/figs/example_education.png" alt="Histogram releases" height="100">](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis)
[<img src="images/figs/example_utility.png" alt="Utility simulations" height="100">](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis)
[<img src="images/figs/example_simulations.png" alt="Bias simulations" height="100">](https://github.com/opendifferentialprivacy/whitenoise-samples/tree/master/analysis)

### WhiteNoise Rust Documentation

The [Rust documentation](https://opendifferentialprivacy.github.io/whitenoise-core/) includes full documentation on all pieces of the library and validator, including extensive [component by component descriptions with examples](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_runtime/components/index.html).

## Communication

(In process.)

## Releases and Contributing

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/whitenoise-core/issues).

We appreciate all contributions. If you are planning to contribute back bug-fixes, please do so without any further discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and discuss the feature with us.
  - Sending a PR without discussion might end up resulting in a rejected PR, because we might be taking the core in a different direction than you might be aware of.

## Contributing Team

Joshua Allen, Christian Covington, Eduardo de Leon, Ira Globus-Harris, James Honaker, Jason Huang, Saniya Movahed, Michael Phelan, Raman Prasad, Michael Shoemate, You?
