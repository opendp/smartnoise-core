[![Build Status](https://travis-ci.org/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.org/opendifferentialprivacy/whitenoise-core)

<a href="http://opendp.io"><img src="images/WhiteNoise Logo/SVG/Full_grey.svg" align="left" height="80" vspace="8" hspace="18"></a>

---

## WhiteNoise Core Differential Privacy

(intro paragraphs here) Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

- [More about WhiteNoise Core](#more-about-whitenoise-core)
  - [Architecture](#architecture)
- [Installation](#installation)
  - [Binaries](#binaries)
  - [From Source](#from-source)
  - [Docker Image](#docker-image)??
- [Getting Started](#getting-started)
  - [Jupyter Notebook Examples](#jupyter-notebook-examples)
  - [WhiteNoise Rust Documentation](#whitenoise-rust-documentation)
- [Communication](#communication)
- [Releases and Contributing](#releases-and-contributing)
- [The Team](#the-team)

---

## More about WhiteNoise Core

- INCLUDE HIGH-LEVEL COMPONENT LIST
- Needs REWRITE / LEFT EXISTING TEXT FOR NOW
- Links to research/whitepapers here?

### Architecture

The runtime and bindings may be written in any language. The core data representation is in protobuf, and the validator is written in Rust. All projects implement protobuf code generation, protobuf serialization/deserialization, communication over FFI, handle distributable packaging, and have at some point compiled cross-platform (more testing needed). All projects communicate via proto definitions from the `prototypes` directory.


#### Validator
The rust validator compiles to binaries that expose C foreign function interfaces and read/automatically generate code for protobuf. A validator C FFI is described in the wiki.

#### Runtimes
The Rust runtime uses a package called ndarray, which feels somewhat like writing numpy in Rust.

#### Bindings
There are two language bindings, one in Python, one in R. Both support building binaries into an installable package.

The Python package is more developed, with helper classes, syntax sugar for building analyses, and visualizations.

The R package uses a shim library in C to interface with compiled binaries. There isn't a programmer interface like in Python yet, but there is a pattern for exposing the C FFI in R code, as well as protobuf generation.

The steps for adding bindings in a new language are essentially:
1. set up package management
2. set up dependency management
3. pack binaries with the given language's tools
4. protobuf code generation
5. FFI implementation and protobuf encoding/decoding
6. write programmer interface


## Installation

### Binaries
[TO DO - python with binaries (pypi) via milksnake]

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


4. Install instructions for the bindings, validator and runtime are located in their respective folders.


---

[WhiteNoise Rust Documentation](https://opendifferentialprivacy.github.io/whitenoise-core/)

## Getting Started

### Jupyter Notebook Examples

Paragraph intro and links to the notebooks

### WhiteNoise Rust Documentation

The [Rust documentation](https://opendifferentialprivacy.github.io/whitenoise-core/) includes component by component descriptions as well as....


## Communication

TO DO

## Releases and Contributing

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/whitenoise-core/issues).

We appreciate all contributions. If you are planning to contribute back bug-fixes, please do so without any further discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and discuss the feature with us.
  - Sending a PR without discussion might end up resulting in a rejected PR, because we might be taking the core in a different direction than you might be aware of.

## The Team



## License
