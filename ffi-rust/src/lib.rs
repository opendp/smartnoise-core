use prost::Message;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::serial::{serialize_error, parse_release, serialize_release};

mod utilities;
use crate::utilities::{ptr_to_buffer, buffer_to_ptr};
use whitenoise_validator::errors::*;
use whitenoise_validator::base::Release;

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
            Ok(request) => {

                let proto::RequestValidateAnalysis {
                    analysis, release
                } = request;

                let run = || -> Result<()> {
                    let analysis = analysis
                        .ok_or_else(|| Error::from("analysis must be defined"))?;
                    let release = parse_release(release
                        .ok_or_else(|| Error::from("release must be defined"))?);

                    whitenoise_validator::validate_analysis(analysis, release)
                };

                match run() {
                    Ok(_) =>
                        Some(proto::response_validate_analysis::Value::Data(proto::response_validate_analysis::Validated {
                            value: true,
                            message: "The analysis is valid.".to_string(),
                        })),
                    Err(err) =>
                        Some(proto::response_validate_analysis::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => {

                let proto::RequestComputePrivacyUsage {
                    analysis, release
                } = request;


                let run = || -> Result<proto::PrivacyUsage> {
                    let analysis = analysis
                        .ok_or_else(|| Error::from("analysis must be defined"))?;
                    let release = parse_release(release
                        .ok_or_else(|| Error::from("release must be defined"))?);

                    whitenoise_validator::compute_privacy_usage(analysis, release)
                };

                match run() {
                    Ok(x) =>
                        Some(proto::response_compute_privacy_usage::Value::Data(x)),
                    Err(err) =>
                        Some(proto::response_compute_privacy_usage::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => {

                let run = || -> Result<String> {
                    let analysis = request.analysis
                        .ok_or_else(|| Error::from("analysis must be defined"))?;
                    let release = parse_release(request.release
                        .ok_or_else(|| Error::from("release must be defined"))?);

                    whitenoise_validator::generate_report(analysis, release)
                };

                match run() {
                    Ok(x) =>
                        Some(proto::response_generate_report::Value::Data(x)),
                    Err(err) =>
                        Some(proto::response_generate_report::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => {

                let proto::RequestAccuracyToPrivacyUsage {
                    component, privacy_definition, properties, accuracies
                } = request;


                let run = || -> Result<proto::PrivacyUsages> {
                    let component: proto::Component = component
                        .ok_or_else(|| Error::from("component must be defined"))?;
                    let privacy_definition: proto::PrivacyDefinition = privacy_definition
                        .ok_or_else(|| Error::from("privacy definition must be defined"))?;
                    let accuracies: proto::Accuracies = accuracies
                        .ok_or_else(|| Error::from("accuracies must be defined"))?;

                    whitenoise_validator::accuracy_to_privacy_usage(
                        component, privacy_definition, properties, accuracies
                    )
                };

                match run() {
                    Ok(x) =>
                        Some(proto::response_accuracy_to_privacy_usage::Value::Data(x)),
                    Err(err) =>
                        Some(proto::response_accuracy_to_privacy_usage::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => {

                let proto::RequestPrivacyUsageToAccuracy {
                    component, privacy_definition, properties, alpha
                } = request;

                let run = || -> Result<proto::Accuracies> {
                    let component: proto::Component = component
                        .ok_or_else(|| Error::from("component must be defined"))?;
                    let privacy_definition: proto::PrivacyDefinition = privacy_definition
                        .ok_or_else(|| Error::from("privacy definition must be defined"))?;

                    whitenoise_validator::privacy_usage_to_accuracy(component, privacy_definition, properties, alpha)
                };

                match run() {
                    Ok(x) =>
                        Some(proto::response_privacy_usage_to_accuracy::Value::Data(x)),
                    Err(err) =>
                        Some(proto::response_privacy_usage_to_accuracy::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => {

                let proto::RequestGetProperties {
                    analysis, release, node_ids
                } = request;

                let run = || -> Result<proto::GraphProperties> {
                    let analysis = analysis
                        .ok_or_else(|| Error::from("analysis must be defined"))?;
                    let release = parse_release(release
                        .ok_or_else(|| Error::from("release must be defined"))?);

                    whitenoise_validator::get_properties(analysis, release, node_ids)
                };

                match run() {
                    Ok(x) =>
                        Some(proto::response_get_properties::Value::Data(x)),
                    Err(err) =>
                        Some(proto::response_get_properties::Value::Error(serialize_error(err))),
                }
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
            Ok(request) => match whitenoise_validator::expand_component(request) {
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

/// FFI wrapper for [release](fn.release.html)
///
/// # Arguments
/// - `request_ptr` - a pointer to an array containing the serialized protobuf of [RequestRelease](proto/struct.RequestRelease.html)
/// - `request_length` - the length of the array
///
/// # Returns
/// a [ByteBufferRuntime struct](struct.ByteBufferRuntime.html) containing a pointer to and length of the serialized protobuf of [proto::ResponseRelease](proto/struct.ResponseRelease.html)
#[cfg(feature = "use-runtime")]
#[no_mangle]
pub extern "C" fn release(
    request_ptr: *const u8, request_length: i32,
) -> ffi_support::ByteBuffer {

    let request_buffer = unsafe { ptr_to_buffer(request_ptr, request_length) };

    let response = proto::ResponseRelease {
        value: match proto::RequestRelease::decode(request_buffer) {
            Ok(request) => {
                let proto::RequestRelease {
                    analysis, release, stack_trace, filter_level
                } = request;


                let run = || -> Result<(Release, Vec<proto::Error>)> {
                    let analysis = analysis
                        .ok_or_else(|| Error::from("analysis must be defined"))?;
                    let release = parse_release(release
                        .ok_or_else(|| Error::from("release must be defined"))?);
                    let filter_level = proto::FilterLevel::from_i32(filter_level)
                        .ok_or_else(|| Error::from(format!("unrecognized filter level {:?}", filter_level)))?;

                    whitenoise_runtime::release(analysis, release, filter_level)
                };

                match run() {
                    Ok((release, warnings)) => Some(proto::response_release::Value::Data(proto::response_release::Success {
                        release: Some(serialize_release(release)),
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
    buffer_to_ptr(response)
}


ffi_support::define_bytebuffer_destructor!(whitenoise_destroy_bytebuffer);
