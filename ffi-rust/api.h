#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// anonymous c struct
typedef struct {
  int64_t len;
  uint8_t *data;
} ByteBuffer;

ByteBuffer accuracy_to_privacy_usage(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer compute_privacy_usage(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer expand_component(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer get_properties(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer generate_report(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer privacy_usage_to_accuracy(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer validate_analysis(const uint8_t *request_ptr, int32_t request_length);

ByteBuffer release(const uint8_t *request_ptr, int32_t request_length);

void whitenoise_destroy_bytebuffer(ByteBuffer buffer);

