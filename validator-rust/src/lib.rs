mod base;
pub mod utilities;
pub mod components;
use base::Component;

// include protobuf-generated traits
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/yarrow.rs"));
}

// define the useful macro for building hashmaps globally
#[macro_export]
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

use prost::Message;
use std::collections::HashMap;

// useful tutorial for proto over ffi here:
// https://github.com/mozilla/application-services/blob/master/docs/howtos/passing-protobuf-data-over-ffi.md
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

#[repr(C)]
#[allow(dead_code)]
struct ByteBuffer {
    len: i64,
    data: *mut u8,
}

#[no_mangle]
pub extern "C" fn validate_analysis(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestValidateAnalysis = prost::Message::decode(request_buffer).unwrap();

    let analysis: proto::Analysis = request.analysis.unwrap();

    let response = proto::ResponseValidateAnalysis {
        value: match base::validate_analysis(&analysis) {
            Ok(x) => Some(proto::response_validate_analysis::Value::Data(x)),
            Err(err) => Some(proto::response_validate_analysis::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

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
            Err(err) => Some(proto::response_compute_privacy_usage::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

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
            Err(err) => Some(proto::response_generate_report::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn accuracy_to_privacy_usage(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestAccuracyToPrivacyUsage = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: proto::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: proto::Component = request.component.unwrap();
    let constraints: HashMap<String, base::Constraint<f64>> = request.constraints.iter()
        .map(|(k, v)| (k.to_owned(), base::Constraint::<f64>::from_proto(&v)))
        .collect();
    let accuracy: proto::Accuracy = request.accuracy.unwrap();

    let privacy_usage: std::result::Result<proto::PrivacyUsage, String> = Ok(component.value.to_owned().unwrap()
        .accuracy_to_privacy_usage(&privacy_definition, &constraints, &accuracy).unwrap());

    let response = proto::ResponseAccuracyToPrivacyUsage {
        value: match privacy_usage {
            Ok(x) => Some(proto::response_accuracy_to_privacy_usage::Value::Data(x)),
            Err(err) => Some(proto::response_accuracy_to_privacy_usage::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn privacy_usage_to_accuracy(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestPrivacyUsageToAccuracy = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: proto::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: proto::Component = request.component.unwrap();
    let constraints: HashMap<String, base::Constraint<f64>> = request.constraints.iter()
        .map(|(k, v)| (k.to_owned(), base::Constraint::<f64>::from_proto(&v)))
        .collect();

    let accuracy: std::result::Result<proto::Accuracy, String> = Ok(proto::Accuracy {
        value: component.value.to_owned().unwrap()
            .privacy_usage_to_accuracy(&privacy_definition, &constraints).unwrap()
    });

    let response = proto::ResponsePrivacyUsageToAccuracy {
        value: match accuracy {
            Ok(x) => Some(proto::response_privacy_usage_to_accuracy::Value::Data(x)),
            Err(err) => Some(proto::response_privacy_usage_to_accuracy::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn expand_graph(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestExpandGraph = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = proto::ResponseExpandGraph {
        value: match base::expand_graph(&analysis, &release) {
            Ok(x) => Some(proto::response_expand_graph::Value::Data(x)),
            Err(err) => Some(proto::response_expand_graph::Value::Error(
                proto::Error { message: err.to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}