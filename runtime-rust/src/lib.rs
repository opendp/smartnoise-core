extern crate yarrow_validator;
use yarrow_validator::yarrow;

mod base;
pub mod utilities;
mod components;

use ndarray::prelude::*;

extern crate libc;
use libc::c_char;

use crate::base::execute_graph;

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
    let request: yarrow::RequestRelease = prost::Message::decode(request_buffer).unwrap();

    let analysis: yarrow::Analysis = request.analysis.unwrap();
    let release: yarrow::Release = request.release.unwrap();
    let dataset: yarrow::Dataset = request.dataset.unwrap();

    let response = yarrow::ResponseRelease {
        value: match base::execute_graph(&analysis, &release, &dataset) {
            Ok(release) => Some(yarrow::response_release::Value::Data(release)),
            Err(err) => Some(yarrow::response_release::Value::Error(
                yarrow::Error{message: err.to_string()}
            ))
        }
    };
    yarrow_validator::buffer_to_ptr(response)
}

//ffi_support::implement_into_ffi_by_protobuf!(yarrow::Release);
ffi_support::define_bytebuffer_destructor!(dp_runtime_destroy_bytebuffer);
