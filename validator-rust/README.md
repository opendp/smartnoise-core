[![Build Status](https://travis-ci.org/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.org/opendifferentialprivacy/whitenoise-core)

## WhiteNoise Core <br/> Differential Privacy Library Validator <br/>

The validator is a sub-project of [Whitenoise-Core](https://github.com/opendifferentialprivacy/whitenoise-core).
See also the accompanying [WhiteNoise-System](https://github.com/opendifferentialprivacy/whitenoise-system) and [WhiteNoise-Samples](https://github.com/opendifferentialprivacy/whitenoise-samples) repositories for this system.

##

Differential privacy is the gold standard definition of privacy protection. The WhiteNoise project aims to connect theoretical solutions from the academic community with the practical lessons learned from real-world deployments, to make differential privacy broadly accessible to future deployments. Specifically, we provide several basic building blocks that can be used by people involved with sensitive data, with implementations based on vetted and mature differential privacy research. In WhiteNoise Core, we provide a pluggable open source library of differentially private algorithms and mechanisms for releasing privacy preserving queries and statistics, as well as APIs for defining an analysis and a validator for evaluating these analyses and composing the total privacy loss on a dataset. 

This library provides a language-agnostic set of utilities for running differentially private analyses. The validator takes in a high-level description of computation, called an Analysis, and checks if the data supplied to each component satisfies requirements necessary to maintain data privacy and derive sensitivities for mechanisms. The validator may also be used to compute the necessary noise scaling/sensitivities under a variety of definitions of privacy, as well as converting to/from accuracy estimates, summarizing releases, and dynamically validating individual components. 

- [More about WhiteNoise Core Runtime](#more-about-whitenoise-core-validator)
  - [Component List](#components)
  - [Architecture](#architecture)
- [WhiteNoise Rust Documentation](#whitenoise-rust-documentation)
- [Communication](#communication)
- [Releases and Contributing](#releases-and-contributing)

---

## More about WhiteNoise Core Validator

### Components

For a full listing of the extensive set of components available in the library [see this documentation.](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_validator/docs/components/index.html)

### Architecture

The Whitenoise-core system architecture [is described in the parent project](https://github.com/opendifferentialprivacy/whitenoise-core#Architecture).
This package provides language-agnostic utilities to aid in implementing differential privacy within your system.
While the computational needs for constructing differentially private statistics may vary broadly depending on the data, the vast majority of differential privacy theory may be applied without ever having access to the data.
The validator is designed such that it never has access to private data, which positions it well as a single library where DP theory may accumulate without concerns about the realities of loading physical databases.
The validator is also designed to work strictly with descriptions of computation in an intermediate protobuf language.

In contrast with the one-and-only validator, there may be many runtimes that can execute an analysis, and there may be many sets of language bindings.
Language bindings are also utility libraries, but for constructing analyses from the context of specific programming languages.

---

### WhiteNoise Rust Documentation

The [Rust documentation](https://opendifferentialprivacy.github.io/whitenoise-core/) includes full documentation on all pieces of the library and validator, including extensive [component by component descriptions with examples](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_runtime/components/index.html).

### Communication

(In process.)

### Releases and Contributing

Please let us know if you encounter a bug by [creating an issue](https://github.com/opendifferentialprivacy/whitenoise-core/issues).

We appreciate all contributions. We welcome pull requests with bug-fixes without prior discussion.

If you plan to contribute new features, utility functions or extensions to the core, please first open an issue and discuss the feature with us.
  - Sending a PR without discussion might end up resulting in a rejected PR, because we might be taking the core in a different direction than you might be aware of.
