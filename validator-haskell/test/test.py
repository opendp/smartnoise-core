# run python test.py from test/
import ctypes

haskell_path = "../.stack-work/install/x86_64-linux/" \
               "148d0e92cd3f02b3b71e5e570acc02f4fd5aeac7a29166dac7a6b62c52d8796b/" \
               "8.6.5/lib/libValidator.so"
validator_lib = ctypes.cdll.LoadLibrary(haskell_path)

validator_lib.DPValidatorInit()
print(validator_lib.foo(7))

validator_lib.showProtos()
