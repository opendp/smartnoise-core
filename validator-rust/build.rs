extern crate prost_build;
//extern crate cbindgen;

use std::env;


fn main() {
    prost_build::compile_protos(
        &[
            "../prototypes/api.proto",
            "../prototypes/base.proto",
            "../prototypes/components.proto",
            "../prototypes/value.proto"
        ],
        &["../prototypes/"]).unwrap();

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    // cbindgen::generate_with_config(
    //     crate_dir,
    //     cbindgen::Config::from_file("cbindgen.toml").unwrap())
    //     .expect("Unable to generate bindings")
    //     .write_to_file("api.h");
}