extern crate prost_build;

fn main() {
    prost_build::compile_protos(
        &[
            "../prototypes/analysis.proto",
            "../prototypes/release.proto",
            "../prototypes/types.proto"
        ],
        &["../prototypes/"]).unwrap();
}