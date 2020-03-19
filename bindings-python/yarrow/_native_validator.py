# auto-generated file
__all__ = ['lib', 'ffi']

import os
from yarrow._native_validator__ffi import ffi

lib = ffi.dlopen(os.path.join(os.path.dirname(__file__), '_native_validator__lib.so'), 4098)
del os
