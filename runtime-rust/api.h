#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
  int64_t len;
  uint8_t *data;
} ByteBufferRuntime;

ByteBufferRuntime release(const uint8_t *request_ptr, int32_t request_length);

void whitenoise_runtime_destroy_bytebuffer(ByteBufferRuntime buffer);
