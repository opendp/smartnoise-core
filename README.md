[![Build Status](https://travis-ci.com/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.com/opendifferentialprivacy/whitenoise-core)

<!---
<a href="http://opendp.io"><img src="images/WhiteNoise Logo/SVG/Full_color.svg" align="left" height="70" vspace="8" hspace="18"></a>
--->

## WhiteNoise Core <br/> Differential Privacy Library <br/>

See also the accompanying [WhiteNoise-System](https://github.com/opendifferentialprivacy/whitenoise-system) and [WhiteNoise-Samples](https://github.com/opendifferentialprivacy/whitenoise-samples) repositories for this system.

---

Differential privacy is the gold standard definition of privacy protection.  The WhiteNoise project aims to connect theoretical solutions from the academic community with the practical lessons learned from real-world deployments, to make differential privacy broadly accessible to future deployments.  Specifically, we provide several basic building blocks that can be used by people involved with sensitive data, with implementations based on vetted and mature differential privacy research.  In WhiteNoise Core, we provide a pluggable open source library of differentially private algorithms and mechanisms for releasing privacy preserving queries and statistics, as well as APIs for defining an analysis and a validator for evaluating these analyses and composing the total privacy loss on a dataset.

The mechanisms library provides a fast, memory-safe native runtime for validating and running differentially private analyses.  The runtime and validator are built in Rust, while Python support is available and R support is forthcoming.

Differentially private computations are specified as an analysis graph that can be validated and executed to produce differentially private releases of data.  Releases include metadata about accuracy of outputs and the complete privacy cost of the analysis.


- [More about WhiteNoise Core](#more-about-whitenoise-core)
  - [Components](#components)
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

#### 1. Validator
  - Location: `/validator-rust`

The core library, is the `validator`, which provides a suite of utilities for checking and deriving necessary conditions for an analysis to be differentially private. This includes checking if sufficient properties have been met for each component, deriving sensitivities, noise scales and accuracies for various definitions of privacy, building reports and dynamically validating individual components. This library is written in Rust.

#### 2. Runtime
  - Location: `/runtime-rust`

There must also be a medium to execute the analysis, called a `runtime`. There is a reference runtime written in Rust, but runtimes may be written using any computation framework--be it SQL, Spark or Dask--to address your individual data needs.

#### 3. Bindings
  - Python Bindings: [whitenoise-core-python](https://github.com/opendifferentialprivacy/whitenoise-core-python)
  - R Bindings (in progress): [whitenoise-core-R](https://github.com/opendifferentialprivacy/whitenoise-core-R)

Finally, there are helper libraries for building analyses, called `bindings`. Bindings may be written for any language, and are thin wrappers over the validator and/or runtime(s). Language bindings are currently available for Python, with support for at minimum R and SQL forthcoming.


#### Note on Protocol Buffers

  - Location: `/validator-rust/prototypes`

Communication among projects is handled via [Protocol Buffer definitions](https://developers.google.com/protocol-buffers/) in the `/validator-rust/prototypes` directory. All three sub-projects implement:
  - Protobuf code generation
  - Protobuf serialization/deserialization
  - Communication over [FFI](https://en.wikipedia.org/wiki/Foreign_function_interface)
  - Handling of distributable packaging

At some point the projects have compiled cross-platform (more testing needed). The validator and reference runtime compile to standalone libraries that may be linked into your project, allowing communication over C foreign function interfaces.

## Installation

### Binaries

#### PyPi packages
- Please see [whitenoise-core-python](https://github.com/opendifferentialprivacy/whitenoise-core-python) which contains python bindings, including links to PyPi packages.

#### Crates.io

The Rust Validator and Runtime are available as crates:
- Validator: [whitenoise_validator](https://crates.io/crates/whitenoise_validator) on crates.io
- Runtime: [whitenoise_runtime](https://crates.io/crates/whitenoise_runtime) on crates.io

### From Source

1. Clone the repository

        git clone $REPOSITORY_URI

    - You may find it easier to use the library with this repository set up as a submodule of the language bindings. In this case, skip this step and refer to the language bindings setup.
    - The default install uses libtools, which requires no spaces in the path to your clone.

2. Install system dependencies (rust, gcc, protoc, python 3.6+ for bindings)

    Mac:

        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
        xcode-select --install
        brew install protobuf python

      You can test with `cargo build` in a new terminal.

    Linux:

        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
        sudo apt-get install diffutils gcc make m4 python
        sudo snap install protobuf --classic

      You can test with `cargo build` in a new terminal.

    Windows:

        choco install rust msys2 protoc python

      For non-Chocolatey users: download and install the latest build of rust, msys2, protobuf and python
      - https://forge.rust-lang.org/infra/other-installation-methods.html
      - https://github.com/protocolbuffers/protobuf/releases/latest
      - https://www.msys2.org/
      - https://www.python.org/downloads/windows/

      Then install gcc under MSYS2

        refreshenv
        reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && setx WN_SYS_ARCH=i686 || setx WN_SYS_ARCH=x86_64
        bash -xlc "pacman --noconfirm -S --needed pacman-mirrors"
        bash -xlc "pacman --noconfirm -S --needed diffutils make mingw-w64-%WN_SYS_ARCH%-gcc"

      You can test with `bash -xc cargo build`. The bash prefix ensures that gmp and mpfr build with the GNU/gcc/mingw toolchain.

3. Optional - Install language bindings
    - [Python](https://github.com/opendifferentialprivacy/whitenoise-core-python#from-source)
    - [R (WIP)](https://github.com/opendifferentialprivacy/whitenoise-core-R#installation)


#### If `cargo build` fails due to the package `gmp-mpfr-sys`

First install system libs (GMP version 6.2, MPFR version 4.0.2-p1)

  Mac:

    brew install gmp mpfr

  Linux:
    Build gmp and mpfr from source. Then set the environment variable:

    export DEP_GMP_OUT_DIR=/path/to/folder/containing/lib/and/includes

  Windows:
    This is not fully tested. Build gmp and mpfr from source. Then set the environment variable and also switch the rust toolchain:

    setx DEP_GMP_OUT_DIR=/path/to/folder/containing/lib/and/includes
    rustup toolchain install stable-%WN_SYS_ARCH%-pc-windows-gnu
    rustup default stable-%WN_SYS_ARCH%-pc-windows-gnu

To install the python bindings, set the variable

    export WN_USE_SYSTEM_LIBS=True

To build the runtime, set the feature flag

    cd runtime-rust; cargo build --feature use-system-libs

#### If `cargo build` fails due to the package `openssl`

Provide an alternative openssl installation, either via directions in the automatic or manual section:
  + https://docs.rs/openssl/0.10.29/openssl/

Otherwise, please open an issue.


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

- Please use [GitHub issues](https://github.com/opendifferentialprivacy/whitenoise-core/issues) for bug reports, feature requests, install issues, and ideas.
- [Gitter](https://gitter.im/opendifferentialprivacy/WhiteNoise) is available for general chat and online discussions.
- For other requests, please contact us at [whitenoise@opendp.io](mailto:whitenoise@opendp.io).
  - _Note: We encourage you to use [GitHub issues](https://github.com/opendifferentialprivacy/whitenoise-core/issues), especially for bugs._

## Releases and Contributing

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/whitenoise-core/issues).

We appreciate all contributions. We welcome pull requests with bug-fixes without prior discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and discuss the feature with us.
  - Sending a PR without discussion might end up resulting in a rejected PR, because we may be taking the core in a different direction than you might be aware of.

## Contributing Team

Joshua Allen, Christian Covington, Eduardo de Leon, Ira Globus-Harris, James Honaker, Jason Huang, Saniya Movahed, Michael Phelan, Raman Prasad, Michael Shoemate, You?
