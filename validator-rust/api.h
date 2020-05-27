#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// anonymous c struct
typedef struct {
  int64_t len;
  uint8_t *data;
} ByteBufferValidator;

ByteBufferValidator accuracy_to_privacy_usage(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator compute_privacy_usage(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator expand_component(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator get_properties(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator generate_report(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator privacy_usage_to_accuracy(const uint8_t *request_ptr, int32_t request_length);

ByteBufferValidator validate_analysis(const uint8_t *request_ptr, int32_t request_length);

void whitenoise_validator_destroy_bytebuffer(ByteBufferValidator buffer);
