from yarrow._native_validator import ffi as ffi_validator, lib as lib_validator
from yarrow._native_runtime import ffi as ffi_runtime, lib as lib_runtime

from . import api_pb2


class LibraryWrapper(object):

    @staticmethod
    def validate_analysis(analysis):
        return _communicate(
            argument=api_pb2.RequestValidateAnalysis(analysis=analysis),
            function=lib_validator.validate_analysis,
            response_type=api_pb2.ResponseValidateAnalysis,
            ffi=ffi_validator)

    @staticmethod
    def compute_privacy_usage(analysis, release):
        return _communicate(
            argument=api_pb2.RequestComputePrivacyUsage(analysis=analysis, release=release),
            function=lib_validator.compute_privacy_usage,
            response_type=api_pb2.ResponseComputePrivacyUsage,
            ffi=ffi_validator)

    @staticmethod
    def generate_report(analysis, release):
        json_string = _communicate(
            argument=api_pb2.RequestGenerateReport(analysis=analysis, release=release),
            function=lib_validator.generate_report,
            response_type=api_pb2.ResponseReport,
            ffi=ffi_validator)

        # TODO: why is ffi returning two extra characters: \n\x10, a newline and data link escape control character?
        return json_string[2:]

    @staticmethod
    def accuracy_to_privacy_usage(privacy_definition, component, constraint, accuracy):
        return _communicate(
            argument=api_pb2.RequestAccuracyToPrivacyUsage(
                privacy_usage=privacy_definition, component=component, constraint=constraint, accuracy=accuracy),
            function=lib_validator.accuracy_to_privacy_usage,
            response_type=api_pb2.RequestAccuracyToPrivacyUsage,
            ffi=ffi_validator)

    @staticmethod
    def privacy_usage_to_accuracy(privacy_definition, component, constraint):
        return _communicate(
            argument=api_pb2.RequestPrivacyUsageToAccuracy(
                privacy_usage=privacy_definition, component=component, constraint=constraint),
            function=lib_validator.privacy_usage_to_accuracy,
            response_type=api_pb2.RequestPrivacyUsageToAccuracy,
            ffi=ffi_validator)

    @staticmethod
    def compute_release(dataset, analysis, release):
        return _communicate(
            argument=api_pb2.RequestRelease(dataset=dataset, analysis=analysis, release=release),
            function=lib_runtime.release,
            response_type=api_pb2.ResponseRelease,
            ffi=ffi_runtime)


def _communicate(function, argument, response_type, ffi):
    """
    Call the function with the proto argument, over the ffi. Deserialize the response and optionally throw an error.
    @param function: function from lib_*
    @param argument: proto object from api.proto
    @param response_type: proto object from api.proto
    @param ffi: one of the ffi_* objects
    @return: the .data field of the protobuf response
    """
    serialized_argument = argument.SerializeToString()

    byte_buffer = function(
        ffi.new(f"uint8_t[{len(serialized_argument)}]", serialized_argument),
        len(serialized_argument))

    serialized_response = ffi.buffer(byte_buffer.data, byte_buffer.len)

    response = response_type.FromString(serialized_response)

    if response.HasField("error"):
        raise response.error
    return response.data