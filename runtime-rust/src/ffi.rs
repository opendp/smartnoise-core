use whitenoise_validator::ffi::serialize_error;
pub use whitenoise_validator::proto;
use prost::Message;

/// Container for responses over FFI.
///
/// The array referenced by this struct contains the serialized value of one protobuf message.
#[repr(C)]
#[allow(dead_code)]
pub struct ByteBufferRuntime {
    /// The length of the array containing serialized protobuf data
    pub len: i64,
    /// Pointer to start of array containing serialized protobuf data
    pub data: *mut u8,
}

/// FFI wrapper for [release](fn.release.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestRelease](proto/struct.RequestRelease.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferRuntime struct](struct.ByteBufferRuntime.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseRelease](proto/struct.ResponseRelease.html)
#[no_mangle]
pub extern "C" fn release(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {
    let request_buffer = unsafe { whitenoise_validator::ffi::ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseRelease {
        value: match proto::RequestRelease::decode(request_buffer) {
            Ok(request) => {
                let stack_trace = request.stack_trace.clone();

                match super::release(request) {
                    Ok((release, warnings)) => Some(proto::response_release::Value::Data(proto::response_release::Success {
                        release: Some(release),
                        warnings: match stack_trace {
                            true => warnings,
                            false => Vec::new()
                        }
                    })),
                    Err(err) => match stack_trace {
                        true =>
                            Some(proto::response_release::Value::Error(serialize_error(err))),
                        false =>
                            Some(proto::response_release::Value::Error(serialize_error("unspecified error while executing analysis".into())))
                    }
                }
            }
            Err(_) => Some(proto::response_release::Value::Error(serialize_error("unable to parse protobuf".into())))
        }
    };
    whitenoise_validator::ffi::buffer_to_ptr(response)
}

ffi_support::define_bytebuffer_destructor!(whitenoise_runtime_destroy_bytebuffer);
