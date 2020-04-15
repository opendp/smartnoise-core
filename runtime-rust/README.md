[![Build Status](https://travis-ci.org/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.org/opendifferentialprivacy/whitenoise-core)

## WhiteNoise Core <br/> Differential Privacy Library Runtime <br/>

This runtime is a sub-project of [Whitenoise-Core](https://github.com/opendifferentialprivacy/whitenoise-core).
See also the accompanying [WhiteNoise-System](https://github.com/opendifferentialprivacy/whitenoise-system) and [WhiteNoise-Samples](https://github.com/opendifferentialprivacy/whitenoise-samples) repositories for this system.

##

Differential privacy is the gold standard definition of privacy protection. The WhiteNoise project aims to connect theoretical solutions from the academic community with the practical lessons learned from real-world deployments, to make differential privacy broadly accessible to future deployments. Specifically, we provide several basic building blocks that can be used by people involved with sensitive data, with implementations based on vetted and mature differential privacy research. In WhiteNoise Core, we provide a pluggable open source library of differentially private algorithms and mechanisms for releasing privacy preserving queries and statistics, as well as APIs for defining an analysis and a validator for evaluating these analyses and composing the total privacy loss on a dataset. 

This library provides a fast, memory-safe native runtime for running differentially private analyses. Differentially private computations are specified as a protobuf analysis graph that can be validated and executed to produce differentially private releases of data.
Releases include metadata about accuracy of outputs and the complete privacy cost of the analysis.


- [More about WhiteNoise Core Runtime](#more-about-whitenoise-core-runtime)
  - [Component List](#components)
  - [Architecture](#architecture)
- [WhiteNoise Rust Documentation](#whitenoise-rust-documentation)
- [Communication](#communication)
- [Releases and Contributing](#releases-and-contributing)

---

## More about WhiteNoise Core Runtime

### Components

For a full listing of the extensive set of components available in the library [see this documentation.](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_validator/docs/components/index.html)

### Architecture

The Whitenoise-core system architecture [is described in the parent project](https://github.com/opendifferentialprivacy/whitenoise-core#Architecture).
This package is an instance of a runtime. The purpose of the runtime is to evaluate components in an analysis on an arbitrary dataset.
This package makes the simplifying assumption that the data is small enough to be loaded in memory. 
The broader system is designed, however, to be able to evaluate components on different runtimes.
For large datasets, it may make more practical sense to conduct non-private transformations and aggregations on a different runtime/backend, and only use the rust runtime for its privatizing mechanisms.  

The necessary public API to use this package is small- only one function, [release](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_runtime/index.html). 
The input to this function is an arbitrarily complex description of computation (analysis) and partial execution of the analysis (also called a release).
Release may be called either [via prost structs](https://opendifferentialprivacy.github.io/whitenoise-core/doc/whitenoise_runtime/proto/index.html) or serialized protobuf.
Utility libraries to construction of the necessary protobufs are called bindings. Bindings are currently only available in Python.

Logic for determining if a component releases differentially private data, as well as the scaling of noise, property tracking, and accuracy estimates are handled by the Validator.

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
