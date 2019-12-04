
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
    analysis_ptr: *const u8, analysis_length: i32
) -> ffi_support::ByteBuffer {

    let analysis_buffer = unsafe {ptr_to_buffer(analysis_ptr, analysis_length)};
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let validation_response: yarrow::Validated = yarrow::Validated {valid: true};
    buffer_to_ptr(validation_response)
}

#[no_mangle]
pub extern "C" fn compute_privacy(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32
) -> ffi_support::ByteBuffer {

    let analysis_buffer = unsafe {ptr_to_buffer(analysis_ptr, analysis_length)};
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe {ptr_to_buffer(release_ptr, release_length)};
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let privacy_usage_response: yarrow::PrivacyUsage = yarrow::PrivacyUsage {};
    buffer_to_ptr(privacy_usage_response)
}

#[no_mangle]
pub extern "C" fn generate_report(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32
) -> ffi_support::ByteBuffer {
    let analysis_buffer = unsafe {ptr_to_buffer(analysis_ptr, analysis_length)};
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe {ptr_to_buffer(release_ptr, release_length)};
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let report_response: yarrow::Report = yarrow::Report {
        value: "{\"key\": \"value\"}".to_owned()
    };
    buffer_to_ptr(report_response)
}

#[no_mangle]
pub extern "C" fn infer_constraints(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32,
    constraints_ptr: *const u8, constraints_length: i32
) -> ffi_support::ByteBuffer {
    let analysis_buffer = unsafe {ptr_to_buffer(analysis_ptr, analysis_length)};
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe {ptr_to_buffer(release_ptr, release_length)};
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let constraints_buffer = unsafe {ptr_to_buffer(constraints_ptr, constraints_length)};
    let constraints: yarrow::Constraints = prost::Message::decode(constraints_buffer).unwrap();

    let analysis_response: yarrow::Analysis = analysis;
    buffer_to_ptr(analysis_response)
}

#[no_mangle]
pub extern "C" fn compute_sensitivities(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32
) -> ffi_support::ByteBuffer {
    let analysis_buffer = unsafe { ptr_to_buffer(analysis_ptr, analysis_length) };
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe { ptr_to_buffer(release_ptr, release_length) };
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let sensitivities_response: yarrow::Sensitivities = yarrow::Sensitivities {};
    buffer_to_ptr(sensitivities_response)
}

#[no_mangle]
pub extern "C" fn from_accuracy(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32,
    accuracy_ptr: *const u8, accuracy_length: i32
) -> ffi_support::ByteBuffer {

    let analysis_buffer = unsafe { ptr_to_buffer(analysis_ptr, analysis_length) };
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe { ptr_to_buffer(release_ptr, release_length) };
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let accuracies_buffer = unsafe { ptr_to_buffer(accuracy_ptr, accuracy_length) };
    let accuracies: yarrow::Accuracies = prost::Message::decode(accuracies_buffer).unwrap();

    let privacy_usage_node_response: yarrow::PrivacyUsageNode = yarrow::PrivacyUsageNode {};
    buffer_to_ptr(privacy_usage_node_response)
}

#[no_mangle]
pub extern "C" fn to_accuracy(
    analysis_ptr: *const u8, analysis_length: i32,
    release_ptr: *const u8, release_length: i32
) -> ffi_support::ByteBuffer {

    let analysis_buffer = unsafe { ptr_to_buffer(analysis_ptr, analysis_length) };
    let analysis: yarrow::Analysis = prost::Message::decode(analysis_buffer).unwrap();

    let release_buffer = unsafe { ptr_to_buffer(release_ptr, release_length) };
    let release: yarrow::Release = prost::Message::decode(release_buffer).unwrap();

    let accuracies_response: yarrow::Accuracies = yarrow::Accuracies {};
    buffer_to_ptr(accuracies_response)
}
