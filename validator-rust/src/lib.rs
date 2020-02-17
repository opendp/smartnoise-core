mod base;
mod utilities;

// include protobuf-generated traits
pub mod yarrow {
    include!(concat!(env!("OUT_DIR"), "/yarrow.rs"));
}

use prost::Message;

// useful tutorial for proto over ffi here:
// https://github.com/mozilla/application-services/blob/master/docs/howtos/passing-protobuf-data-over-ffi.md
unsafe fn ptr_to_buffer<'a>(data: *const u8, len: i32) -> &'a [u8] {
    assert!(len >= 0, "Bad buffer len: {}", len);
    if len == 0 {
        // This will still fail, but as a bad protobuf format.
        &[]
    } else {
        assert!(!data.is_null(), "Unexpected null data pointer");
        std::slice::from_raw_parts(data, len as usize)
    }
}

fn buffer_to_ptr<T>(buffer: T) -> ffi_support::ByteBuffer
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
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestValidateAnalysis = prost::Message::decode(request_buffer).unwrap();

    let analysis: yarrow::Analysis = request.analysis.unwrap();

    let response = yarrow::ResponseValidateAnalysis {
        value: match base::validate_analysis(&analysis) {
            Ok(x) => Some(yarrow::response_validate_analysis::Value::Data(x)),
            Err(err) => Some(yarrow::response_validate_analysis::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn compute_privacy_usage(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestComputePrivacyUsage = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = yarrow::ResponseComputePrivacyUsage {
        value: match base::compute_privacy_usage(&analysis, &release) {
            Ok(x) => Some(yarrow::response_compute_privacy_usage::Value::Data(x)),
            Err(err) => Some(yarrow::response_compute_privacy_usage::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn generate_report(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestGenerateReport = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = yarrow::ResponseGenerateReport {
        value: match base::generate_report(&analysis, &release) {
            Ok(x) => Some(yarrow::response_generate_report::Value::Data(x)),
            Err(err) => Some(yarrow::response_generate_report::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn accuracy_to_privacy_usage(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestAccuracyToPrivacyUsage = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: yarrow::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: yarrow::Component = request.component.unwrap();
    let constraint: yarrow::Constraint = request.constraint.unwrap();
    let accuracy: yarrow::Accuracy = request.accuracy.unwrap();

    let response = yarrow::ResponseAccuracyToPrivacyUsage {
        value: match base::accuracy_to_privacy_usage(&privacy_definition, &component, &constraint, &accuracy) {
            Ok(x) => Some(yarrow::response_accuracy_to_privacy_usage::Value::Data(x)),
            Err(err) => Some(yarrow::response_accuracy_to_privacy_usage::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn privacy_usage_to_accuracy(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestPrivacyUsageToAccuracy = prost::Message::decode(request_buffer).unwrap();

    let privacy_definition: yarrow::PrivacyDefinition = request.privacy_definition.unwrap();
    let component: yarrow::Component = request.component.unwrap();
    let constraint: yarrow::Constraint = request.constraint.unwrap();

    let response = yarrow::ResponsePrivacyUsageToAccuracy {
        value: match base::privacy_usage_to_accuracy(&privacy_definition, &component, &constraint) {
            Ok(x) => Some(yarrow::response_privacy_usage_to_accuracy::Value::Data(x)),
            Err(err) => Some(yarrow::response_privacy_usage_to_accuracy::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn expand_graph(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {ptr_to_buffer(request_ptr, request_length)};
    let request: yarrow::RequestExpandGraph = prost::Message::decode(request_buffer).unwrap();

    let analysis = request.analysis.unwrap();
    let release = request.release.unwrap();

    let response = yarrow::ResponseExpandGraph {
        value: match base::expand_graph(&analysis, &release) {
            Ok(x) => Some(yarrow::response_expand_graph::Value::Data(x)),
            Err(err) => Some(yarrow::response_expand_graph::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    buffer_to_ptr(response)
}