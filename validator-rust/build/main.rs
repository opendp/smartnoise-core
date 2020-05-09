extern crate prost_build;
//extern crate cbindgen;

mod bindings;
mod protobuf;
mod documentation;

use std::io;
use std::path::Path;
use std::fs;
use std::env;
use std::path::PathBuf;

extern crate serde;
extern crate serde_json;

use serde::Deserialize;

use std::fs::File;
use std::io::Read;
// BTreeMaps preserve the order of keys. HashMaps don't preserve the order of keys.
// Since components.proto is rebuilt every time validator-rust is compiled,
//     the proto field ids are shuffled if options are stored in a HashMap
// Options are stored in a BTreeMap to prevent desynchronization of the proto ids
//     between the validator build, and the validator build as a dependency of the runtime
use std::collections::BTreeMap;
use std::ffi::OsStr;

extern crate build_deps;


#[derive(Deserialize, Debug)]
pub struct ComponentJSON {
    id: String,
    name: String,
    arguments: BTreeMap<String, ArgumentJSON>,
    options: BTreeMap<String, ArgumentJSON>,
    #[serde(rename(serialize = "return", deserialize = "return"))]
    arg_return: ArgumentJSON,
    description: Option<String>,
    proto_id: i64
}

#[derive(Deserialize, Debug)]
pub struct ArgumentJSON {
    nature: Option<Vec<String>>,
    type_value: Option<String>,
    type_proto: Option<String>,
    type_rust: Option<String>,
    default_rust: Option<String>,
    default_python: Option<String>,
    description: Option<String>,
}


fn main() {

    // Load proto paths
    let proto_dir = PathBuf::from("./prototypes");
    if !proto_dir.exists() {
        panic!("Failed to find the prototypes directory.");
    }

    let components_dir = proto_dir.join("components");

    // Enumerate component json files as relevant resources to the compiler
    // Adding the parent directory "components" to the watch-list will capture new-files being added
    build_deps::rerun_if_changed_paths(proto_dir.to_str().unwrap()).unwrap();
    build_deps::rerun_if_changed_paths(proto_dir.join("*").to_str().unwrap()).unwrap();

    build_deps::rerun_if_changed_paths(components_dir.to_str().unwrap()).unwrap();
    build_deps::rerun_if_changed_paths(components_dir.join("*").to_str().unwrap()).unwrap();

    // load components
    let mut components = fs::read_dir(&Path::new(&components_dir))
        .expect("components directory was not found")
        // ignore invalid dirs
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension() == Some(OsStr::new("json")))
        .map(|entry| {
            let mut file: File = File::open(entry.path())?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;

            // Deserialize and print Rust data structure.
            let data: ComponentJSON = serde_json::from_str(&contents)?;
            Ok(data)
        })
        .collect::<Result<Vec<ComponentJSON>, io::Error>>().unwrap();

    components.sort_by_key(|component| component.name.clone());

    // code generation
    let out_dir = PathBuf::from(&env::var("OUT_DIR").unwrap());

    bindings::build_bindings(&components,
                             out_dir.join("bindings_analysis.rs"),
                             out_dir.join("bindings_builders.rs"));
    documentation::build_documentation(&components, out_dir.join("components.rs"));
    protobuf::build_protobuf(&components, proto_dir.join("components.proto"));

    prost_build::Config::new().compile_protos(
        &[
            proto_dir.join("api.proto"),
            proto_dir.join("base.proto"),
            proto_dir.join("components.proto"),
            proto_dir.join("value.proto")
        ],
        &[proto_dir]).unwrap();


//    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
//
//    cbindgen::generate_with_config(
//        crate_dir,
//        cbindgen::Config::from_file("cbindgen.toml").unwrap())
//        .expect("Unable to generate bindings")
//        .write_to_file("./api.h");

    // panic to prevent stdout from being masked
    // panic!("You can't suppress me rustc!");
}
