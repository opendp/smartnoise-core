
// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;

#[doc(hidden)]
pub mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}

#[doc(hidden)]
pub use errors::*;
use error_chain::ChainedError;
use std::io::Write; // trait which holds `display_chain`

#[doc(hidden)]
pub static ERR_STDERR: &'static str = "Error writing to stderr";

pub mod base;
pub mod utilities;
pub mod components;
use crate::components::*;

// include protobuf-generated traits
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/whitenoise.rs"));
}

// define the useful macro for building hashmaps globally
#[macro_export]
#[doc(hidden)]
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         #[allow(unused_mut)]
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

use prost::Message;
use std::collections::HashMap;

use crate::base::Value;
use crate::utilities::serial::parse_value;

// useful tutorial for proto over ffi here:
// https://github.com/mozilla/application-services/blob/master/docs/howtos/passing-protobuf-data-over-ffi.md
#[doc(hidden)]
pub unsafe fn ptr_to_buffer<'a>(data: *const u8, len: i32) -> &'a [u8] {
    assert!(len >= 0, "Bad buffer len: {}", len);
    if len == 0 {
        // This will still fail, but as a bad protobuf format.
        &[]
    } else {
        assert!(!data.is_null(), "Unexpected null data pointer");
        std::slice::from_raw_parts(data, len as usize)
    }
}

#[doc(hidden)]
pub fn buffer_to_ptr<T>(buffer: T) -> ffi_support::ByteBuffer
    where T: Message {
    let mut out_buffer = Vec::new();
    match prost::Message::encode(&buffer, &mut out_buffer) {
        Ok(_t) => ffi_support::ByteBuffer::from_vec(out_buffer),
        Err(error) => {
            println!("Error encoding response protobuf.");
            println!("{:?}", error);
            ffi_support::ByteBuffer::new_with_size(0)
        }
    }
}

/// Container for responses over FFI.
///
/// The array referenced by this struct contains the serialized value of one protobuf message.
#[repr(C)]
#[allow(dead_code)]
pub struct ByteBuffer {
    /// The length of the array containing serialized protobuf data
    pub len: i64,
    /// Pointer to start of array containing serialized protobuf data
    pub data: *mut u8,
}

/// Validate if an analysis is well-formed.
///
/// Checks that the graph is a DAG.
/// Checks that static properties are met on all components.
///
/// Useful for static validation of an analysis.
/// Since some components require public arguments, mechanisms that depend on other mechanisms cannot be verified until the components they depend on have been validated.
///
/// The system may also be run dynamically- prior to expanding each node, calling the expand_component endpoint will also validate the component being expanded.
/// NOTE: Evaluating the graph dynamically opens up additional potential timing attacks.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestValidateAnalysis](proto/struct.RequestValidateAnalysis.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseValidateAnalysis](proto/struct.ResponseValidateAnalysis.html)
#[no_mangle]
pub extern "C" fn validate_analysis(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestValidateAnalysis = prost::Message::decode(request_buffer).unwrap();

    let analysis: proto::Analysis = request.analysis.unwrap();
    let release: proto::Release = match request.release {
        Some(value) => value, None => proto::Release {values: HashMap::new()}
    };

    let response = proto::ResponseValidateAnalysis {
        value: match base::validate_analysis(&analysis, &release) {
            Ok(x) => Some(proto::response_validate_analysis::Value::Data(x)),
            Err(err) => {
//
//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_validate_analysis::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}


/// Compute overall privacy usage of an analysis.
///
/// The privacy usage is sum of the privacy usages for each node.
/// The Release's actual privacy usage, if defined, takes priority over the maximum allowable privacy usage defined in the Analysis.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestComputePrivacyUsage](proto/struct.RequestComputePrivacyUsage.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseComputePrivacyUsage](proto/struct.ResponseComputePrivacyUsage.html)
#[no_mangle]
pub extern "C" fn compute_privacy_usage(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestComputePrivacyUsage = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = proto::ResponseComputePrivacyUsage {
        value: match base::compute_privacy_usage(&analysis, &release) {
            Ok(x) => Some(proto::response_compute_privacy_usage::Value::Data(x)),
            Err(err) => {

//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_compute_privacy_usage::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}


/// Generate a json string with a summary/report of the Analysis and Release
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestGenerateReport](proto/struct.RequestGenerateReport.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseGenerateReport](proto/struct.ResponseGenerateReport.html)
#[no_mangle]
pub extern "C" fn generate_report(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestGenerateReport = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = proto::ResponseGenerateReport {
        value: match base::generate_report(&analysis, &release) {
            Ok(x) => Some(proto::response_generate_report::Value::Data(x)),
            Err(err) => {

//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_generate_report::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}


/// TODO: Estimate the privacy usage necessary to bound accuracy to a given value.
///
/// No context about the analysis is necessary, just the privacy definition and properties of the arguments of the component.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestAccuracyToPrivacyUsage](proto/struct.RequestAccuracyToPrivacyUsage.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseAccuracyToPrivacyUsage](proto/struct.ResponseAccuracyToPrivacyUsage.html)
#[no_mangle]
pub extern "C" fn accuracy_to_privacy_usage(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestAccuracyToPrivacyUsage = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: proto::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: proto::Component = request.component.unwrap();
    let properties: HashMap<String, base::ValueProperties> = request.properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();
    let accuracy: proto::Accuracy = request.accuracy.unwrap();

    let privacy_usage: Result<proto::PrivacyUsage> = Ok(component.variant.to_owned().unwrap()
        .accuracy_to_privacy_usage(&privacy_definition, &properties, &accuracy).unwrap());

    let response = proto::ResponseAccuracyToPrivacyUsage {
        value: match privacy_usage {
            Ok(x) => Some(proto::response_accuracy_to_privacy_usage::Value::Data(x)),
            Err(err) => {

//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_accuracy_to_privacy_usage::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}


/// TODO: Estimate the accuracy of the release of a component, based on a privacy usage.
///
/// No context about the analysis is necessary, just the properties of the arguments of the component.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestPrivacyUsageToAccuracy](proto/struct.RequestPrivacyUsageToAccuracy.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponsePrivacyUsageToAccuracy](proto/struct.ResponsePrivacyUsageToAccuracy.html)
#[no_mangle]
pub extern "C" fn privacy_usage_to_accuracy(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestPrivacyUsageToAccuracy = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: proto::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: proto::Component = request.component.unwrap();
    let properties: HashMap<String, base::ValueProperties> = request.properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();

    let accuracy: Result<proto::Accuracy> = Ok(proto::Accuracy {
        value: component.variant.to_owned().unwrap()
            .privacy_usage_to_accuracy(&privacy_definition, &properties).unwrap()
    });

    let response = proto::ResponsePrivacyUsageToAccuracy {
        value: match accuracy {
            Ok(x) => Some(proto::response_privacy_usage_to_accuracy::Value::Data(x)),
            Err(err) => {

//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_privacy_usage_to_accuracy::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}


/// Expand a component that may be representable as smaller components, and propagate its properties.
///
/// This is function may be called interactively from the runtime as the runtime executes the computational graph, to allow for dynamic graph validation.
/// This is opposed to statically validating a graph, where the nodes in the graph that are dependent on the releases of mechanisms cannot be known and validated until the first release is made.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestExpandComponent](proto/struct.RequestExpandComponent.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseExpandComponent](proto/struct.ResponseExpandComponent.html)
#[no_mangle]
pub extern "C" fn expand_component(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestExpandComponent = prost::Message::decode(request_buffer).unwrap();

    let component: proto::Component = request.component.unwrap();
    let arguments: HashMap<String, Value> = request.arguments.iter()
        .map(|(k, v)| (k.to_owned(), parse_value(&v).unwrap()))
        .collect();
    let privacy_definition: proto::PrivacyDefinition = request.privacy_definition.unwrap();

    let response = proto::ResponseExpandComponent {
        value: match base::expand_component(
            &privacy_definition,
            &component,
            &request.properties,
            &arguments,
            request.component_id,
            request.maximum_id
        ) {
            Ok(x) => Some(proto::response_expand_component::Value::Data(x)),
            Err(err) => {

//                let stderr = &mut ::std::io::stderr();
//                writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                ::std::process::exit(1);

                Some(proto::response_expand_component::Value::Error(
                    proto::Error { message: err.display_chain().to_string() }
                ))
            }
        }
    };
    buffer_to_ptr(response)
}
