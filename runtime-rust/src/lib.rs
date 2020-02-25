extern crate yarrow_validator;
use yarrow_validator::proto;

mod base;
mod utilities;
mod components;



extern crate libc;




#[repr(C)]
#[allow(dead_code)]
struct ByteBuffer {
    len: i64,
    data: *mut u8,
}

#[no_mangle]
pub extern "C" fn release(
    request_ptr: *const u8, request_length: i32
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe {yarrow_validator::ptr_to_buffer(request_ptr, request_length)};
    let request: proto::RequestRelease = prost::Message::decode(request_buffer).unwrap();

    let analysis: proto::Analysis = request.analysis.unwrap();
    let release: proto::Release = request.release.unwrap();
    let dataset: proto::Dataset = request.dataset.unwrap();

    let response = proto::ResponseRelease {
        value: match base::execute_graph(&analysis, &release, &dataset) {
            Ok(release) => Some(proto::response_release::Value::Data(release)),
            Err(err) => Some(proto::response_release::Value::Error(
                proto::Error{message: err}
            ))
        }
    };
    yarrow_validator::buffer_to_ptr(response)
}

//ffi_support::implement_into_ffi_by_protobuf!(proto::Release);
ffi_support::define_bytebuffer_destructor!(dp_runtime_destroy_bytebuffer);
