//! Foreign function interfaces

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

/// FFI wrapper for [validate_analysis](../fn.validate_analysis.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestValidateAnalysis](../proto/struct.RequestValidateAnalysis.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseValidateAnalysis](../proto/struct.ResponseValidateAnalysis.html)
#[no_mangle]
pub extern "C" fn validate_analysis(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseValidateAnalysis {
        value: match proto::RequestValidateAnalysis::decode(request_buffer) {
            Ok(request) => match super::validate_analysis(request) {
                Ok(x) =>
                    Some(proto::response_validate_analysis::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_validate_analysis::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_validate_analysis::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

/// FFI wrapper for [compute_privacy_usage](../fn.compute_privacy_usage.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestComputePrivacyUsage](../proto/struct.RequestComputePrivacyUsage.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseComputePrivacyUsage](../proto/struct.ResponseComputePrivacyUsage.html)
#[no_mangle]
pub extern "C" fn compute_privacy_usage(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseComputePrivacyUsage {
        value: match proto::RequestComputePrivacyUsage::decode(request_buffer) {
            Ok(request) => match super::compute_privacy_usage(request) {
                Ok(x) =>
                    Some(proto::response_compute_privacy_usage::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_compute_privacy_usage::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_compute_privacy_usage::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

/// FFI wrapper for [generate_report](../fn.generate_report.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestGenerateReport](../proto/struct.RequestGenerateReport.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseGenerateReport](../proto/struct.ResponseGenerateReport.html)
#[no_mangle]
pub extern "C" fn generate_report(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseGenerateReport {
        value: match proto::RequestGenerateReport::decode(request_buffer) {
            Ok(request) => match super::generate_report(request) {
                Ok(x) =>
                    Some(proto::response_generate_report::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_generate_report::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_generate_report::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

/// FFI wrapper for [accuracy_to_privacy_usage](../fn.accuracy_to_privacy_usage.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestAccuracyToPrivacyUsage](../proto/struct.RequestAccuracyToPrivacyUsage.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseAccuracyToPrivacyUsage](../proto/struct.ResponseAccuracyToPrivacyUsage.html)
#[no_mangle]
pub extern "C" fn accuracy_to_privacy_usage(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseAccuracyToPrivacyUsage {
        value: match proto::RequestAccuracyToPrivacyUsage::decode(request_buffer) {
            Ok(request) => match super::accuracy_to_privacy_usage(request) {
                Ok(x) =>
                    Some(proto::response_accuracy_to_privacy_usage::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_accuracy_to_privacy_usage::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_accuracy_to_privacy_usage::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };

    buffer_to_ptr(response)
}

/// FFI wrapper for [privacy_usage_to_accuracy](../fn.privacy_usage_to_accuracy.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestPrivacyUsageToAccuracy](../proto/struct.RequestPrivacyUsageToAccuracy.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponsePrivacyUsageToAccuracy](../proto/struct.ResponsePrivacyUsageToAccuracy.html)
#[no_mangle]
pub extern "C" fn privacy_usage_to_accuracy(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponsePrivacyUsageToAccuracy {
        value: match proto::RequestPrivacyUsageToAccuracy::decode(request_buffer) {
            Ok(request) => match super::privacy_usage_to_accuracy(request) {
                Ok(x) =>
                    Some(proto::response_privacy_usage_to_accuracy::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_privacy_usage_to_accuracy::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_privacy_usage_to_accuracy::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

/// FFI wrapper for [get_properties](../fn.get_properties.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestGetProperties](../proto/struct.RequestGetProperties.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseGetProperties](../proto/struct.ResponseGetProperties.html)
#[no_mangle]
pub extern "C" fn get_properties(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseGetProperties {
        value: match proto::RequestGetProperties::decode(request_buffer) {
            Ok(request) => match super::get_properties(request) {
                Ok(x) =>
                    Some(proto::response_get_properties::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_get_properties::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_get_properties::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

/// FFI wrapper for [expand_component](../fn.expand_component.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestExpandComponent](../proto/struct.RequestExpandComponent.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferValidator struct](struct.ByteBufferValidator.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseExpandComponent](../proto/struct.ResponseExpandComponent.html)
#[no_mangle]
pub extern "C" fn expand_component(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseExpandComponent {
        value: match proto::RequestExpandComponent::decode(request_buffer) {
            Ok(request) => match super::expand_component(request) {
                Ok(x) =>
                    Some(proto::response_expand_component::Value::Data(x)),
                Err(err) =>
                    Some(proto::response_expand_component::Value::Error(serialize_error(err))),
            }
            Err(_) =>
                Some(proto::response_expand_component::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    buffer_to_ptr(response)
}

#[doc(hidden)]
pub fn serialize_error(err: super::Error) -> proto::Error {
    proto::Error { message: err.display_chain().to_string() }
}

ffi_support::define_bytebuffer_destructor!(whitenoise_validator_destroy_bytebuffer);
