extern crate yarrow_validator;
use yarrow_validator::yarrow;

mod base;
mod utilities;
mod components;

use ndarray::prelude::*;

extern crate libc;
use libc::c_char;

use crate::base::execute_graph;

// useful tutorial for proto over ffi here:
// https://github.com/mozilla/application-services/blob/master/docs/howtos/passing-protobuf-data-over-ffi.md
unsafe fn get_buffer<'a>(data: *const u8, len: i32) -> &'a [u8] {
    assert!(len >= 0, "Bad buffer len: {}", len);
    if len == 0 {
        // This will still fail, but as a bad protobuf format.
        &[]
    } else {
        assert!(!data.is_null(), "Unexpected null data pointer");
        std::slice::from_raw_parts(data, len as usize)
    }
}

#[repr(C)]
#[allow(dead_code)]
struct ByteBuffer {
    len: i64,
    data: *mut u8,
}

#[no_mangle]
pub extern "C" fn release(
    dataset_ptr: *const u8, dataset_length: i32,
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32
) -> ffi_support::ByteBuffer {

    let dataset_buffer = unsafe {get_buffer(dataset_ptr, dataset_length)};
    let dataset: yarrow::Dataset = prost::Message::decode(dataset_buffer).unwrap();

    let analysis_buffer = unsafe {get_buffer(analysis_ptr, analysis_length)};
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe {get_buffer(release_ptr, release_length)};
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let response_release = execute_graph(&analysis, &release, &dataset);

    let response_results_release = match response_release {
        Ok(release) => yarrow::ResultRelease {
            value: Some(yarrow::result_release::Value::Release(release))
        },
        Err(message) => yarrow::ResultRelease {
            value: Some(yarrow::result_release::Value::Error(yarrow::Error {message: message.to_owned()}))
        }
    };

    let mut out_buffer = Vec::new();
    match prost::Message::encode(&response_results_release, &mut out_buffer) {
        Ok(_t) => ffi_support::ByteBuffer::from_vec(out_buffer),
        Err(error) => {
            println!("Error encoding response protobuf.");
            println!("{:?}", error);
            ffi_support::ByteBuffer::new_with_size(0)
        }
    }
}

//ffi_support::implement_into_ffi_by_protobuf!(yarrow::Release);
ffi_support::define_bytebuffer_destructor!(dp_runtime_destroy_bytebuffer);
