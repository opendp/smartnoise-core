from tests.test_base import (
    test_basic_path,
    test_raw_dataset,
)

# analysis = test_basic_path()
analysis = test_raw_dataset()


print('analysis is valid:', analysis.validate())

print('epsilon:', analysis.epsilon)

print('release json:', analysis.release())
print('release proto:', analysis.release_proto)
analysis.plot()
