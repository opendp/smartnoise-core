from whitenoise._native_validator import ffi as ffi_validator, lib as lib_validator
from whitenoise._native_runtime import ffi as ffi_runtime, lib as lib_runtime

from . import api_pb2
import re
import platform


class LibraryWrapper(object):

    @staticmethod
    def validate_analysis(analysis, release):
        return _communicate(
            argument=api_pb2.RequestValidateAnalysis(analysis=analysis, release=release),
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
        return _communicate(
            argument=api_pb2.RequestGenerateReport(analysis=analysis, release=release),
            function=lib_validator.generate_report,
            response_type=api_pb2.ResponseGenerateReport,
            ffi=ffi_validator)

    @staticmethod
    def accuracy_to_privacy_usage(privacy_definition, component, properties, accuracy):
        return _communicate(
            argument=api_pb2.RequestAccuracyToPrivacyUsage(
                privacy_definition=privacy_definition, component=component, properties=properties, accuracy=accuracy),
            function=lib_validator.accuracy_to_privacy_usage,
            response_type=api_pb2.RequestAccuracyToPrivacyUsage,
            ffi=ffi_validator)

    @staticmethod
    def privacy_usage_to_accuracy(privacy_definition, component, properties, alpha):
        return _communicate(
            argument=api_pb2.RequestPrivacyUsageToAccuracy(
                privacy_definition=privacy_definition, component=component, properties=properties, alpha=alpha),
            function=lib_validator.privacy_usage_to_accuracy,
            response_type=api_pb2.RequestPrivacyUsageToAccuracy,
            ffi=ffi_validator)

    @staticmethod
    def get_properties(analysis, release):
        return _communicate(
            argument=api_pb2.RequestGetProperties(analysis=analysis, release=release),
            function=lib_validator.get_properties,
            response_type=api_pb2.ResponseGetProperties,
            ffi=ffi_validator)

    @staticmethod
    def compute_release(analysis, release):
        return _communicate(
            argument=api_pb2.RequestRelease(
                analysis=analysis,
                release=release,
                stack_trace=True),
            function=lib_runtime.release,
            response_type=api_pb2.ResponseRelease,
            ffi=ffi_runtime)


def _communicate(function, argument, response_type, ffi):
    """
    Call the function with the proto argument, over ffi. Deserialize the response and optionally throw an error.
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

    # Errors from here are propagated up from either the rust validator or runtime
    if response.HasField("error"):

        library_traceback = response.error.message

        # noinspection PyBroadException
        try:
            # on Linux, stack traces are more descriptive
            if platform.system() == "Linux":
                message, *frames = re.split("\n +[0-9]+: ", library_traceback)
                library_traceback = '\n'.join(reversed(["  " + frame.replace("         at", "at") for frame in frames
                                                        if ("at src/" in frame or "whitenoise_validator" in frame)
                                                        and "whitenoise_validator::errors::Error" not in frame])) \
                                    + "\n  " + message
        except Exception:
            pass

        # stack traces beyond this point come from the internal rust libraries
        raise RuntimeError(library_traceback)
    return response.data
