use libc::{c_char, c_void, strdup};
use std::ffi::CStr;

// Include the `items` module, which is generated from items.proto.
pub mod burdock {
    include!(concat!(env!("OUT_DIR"), "/burdock.rs"));
}

#[no_mangle]
pub extern "C" fn release(
    analysis_buffer: *const c_char, analysis_length: i32,
    release_buffer: *const c_char, release_length: i32,
    data_path: *const c_char, data_path_length: i32,
    header: *const c_char, header_length: i32) -> *const c_char {

    let c_str_analysis: &CStr = unsafe { CStr::from_ptr(analysis_buffer) };
    let analysis = <burdock::Analysis as prost::Message>::decode(c_str_analysis.to_bytes().to_vec());

    let c_str_release: &CStr = unsafe { CStr::from_ptr(release_buffer) };
    let release = <burdock::Analysis as prost::Message>::decode(c_str_release.to_bytes().to_vec());

    println!("{:?}", analysis);
    return unsafe { strdup(release_buffer) };
}

#[no_mangle]
pub extern "C" fn releaseArray(
    analysis_buffer: *const c_char, analysis_length: i32,
    release_buffer: *const c_char, release_length: i32,
    m: i32, n: i32, data: *const*const f64,
    header: *const c_char, header_length: i32) -> *const c_char {

    let c_str_analysis: &CStr = unsafe { CStr::from_ptr(analysis_buffer) };
    let analysis = <burdock::Analysis as prost::Message>::decode(c_str_analysis.to_bytes().to_vec());

    let c_str_release: &CStr = unsafe { CStr::from_ptr(release_buffer) };
    let release = <burdock::Analysis as prost::Message>::decode(c_str_release.to_bytes().to_vec());

    return unsafe { strdup(release_buffer) };
}

#[no_mangle]
pub extern "C" fn freePtr(buffer: *const c_char) {
//    unsafe { libc::free(buffer)};
}