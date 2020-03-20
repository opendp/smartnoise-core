/// The Whitenoise rust runtime is an execution engine for evaluating differentially private analyses.

extern crate whitenoise_validator;

pub use whitenoise_validator::proto;

 // trait which holds `display_chain`
use error_chain::ChainedError;
pub mod base;
pub mod utilities;
pub mod components;

extern crate libc;


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


/// Evaluate an analysis and release the differentially private results.
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestRelease](proto/struct.RequestRelease.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBuffer struct](struct.ByteBuffer.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseRelease](proto/struct.ResponseRelease.html)
#[no_mangle]
pub extern "C" fn release(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {whitenoise_validator::ptr_to_buffer(request_ptr, request_length)};
    let request: proto::RequestRelease = prost::Message::decode(request_buffer).unwrap();

    let analysis: proto::Analysis = request.analysis.unwrap();
    let release: proto::Release = request.release.unwrap();

    let response = proto::ResponseRelease {
        value: match base::execute_graph(&analysis, &release) {
            Ok(release) => Some(proto::response_release::Value::Data(release)),
            Err(err) => {

                if request.stack_trace {
//                    let stderr = &mut ::std::io::stderr();
//                    writeln!(stderr, "{}", err.display_chain()).expect(ERR_STDERR);
//                    ::std::process::exit(1);

                    Some(proto::response_release::Value::Error(
                        proto::Error { message: err.display_chain().to_string() }
                    ))

                } else {
                    Some(proto::response_release::Value::Error(
                        proto::Error { message: "unspecified error while executing analysis".to_string() }
                    ))
                }
            }
        }
    };
    whitenoise_validator::buffer_to_ptr(response)
}

//ffi_support::implement_into_ffi_by_protobuf!(proto::Release);
//ffi_support::define_bytebuffer_destructor!(dp_runtime_destroy_bytebuffer);
