
use crate::proto;
use error_chain::ChainedError;
use prost::Message;

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

    let response = proto::ResponseValidateAnalysis {
        value: match super::validate_analysis(&request) {
            Ok(x) => Some(proto::response_validate_analysis::Value::Data(x)),
            Err(err) => Some(proto::response_validate_analysis::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
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

    let response = proto::ResponseComputePrivacyUsage {
        value: match super::compute_privacy_usage(&request) {
            Ok(x) => Some(proto::response_compute_privacy_usage::Value::Data(x)),
            Err(err) => Some(proto::response_compute_privacy_usage::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
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

    let response = proto::ResponseGenerateReport {
        value: match super::generate_report(&request) {
            Ok(x) => Some(proto::response_generate_report::Value::Data(x)),
            Err(err) => Some(proto::response_generate_report::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}


/// Estimate the privacy usage necessary to bound accuracy to a given value.
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

    let response = proto::ResponseAccuracyToPrivacyUsage {
        value: match super::accuracy_to_privacy_usage(&request) {
            Ok(x) => Some(proto::response_accuracy_to_privacy_usage::Value::Data(x)),
            Err(err) => Some(proto::response_accuracy_to_privacy_usage::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}


/// Estimate the accuracy of the release of a component, based on a privacy usage.
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

    let response = proto::ResponsePrivacyUsageToAccuracy {
        value: match super::privacy_usage_to_accuracy(&request) {
            Ok(x) => Some(proto::response_privacy_usage_to_accuracy::Value::Data(x)),
            Err(err) => Some(proto::response_privacy_usage_to_accuracy::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}

#[no_mangle]
pub extern "C" fn get_properties(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };
    let request: proto::RequestGetProperties = prost::Message::decode(request_buffer).unwrap();

    let response = proto::ResponseGetProperties {
        value: match super::get_properties(&request) {
            Ok(x) => Some(proto::response_get_properties::Value::Data(x)),
            Err(err) => Some(proto::response_get_properties::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
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

    let response = proto::ResponseExpandComponent {
        value: match super::expand_component(&request) {
            Ok(x) => Some(proto::response_expand_component::Value::Data(x)),
            Err(err) => Some(proto::response_expand_component::Value::Error(
                proto::Error { message: err.display_chain().to_string() }
            ))
        }
    };
    buffer_to_ptr(response)
}
