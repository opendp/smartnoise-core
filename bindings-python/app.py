from tests.test_base import (
    test_basic_path,
    test_dp_mean,
    test_raw_dataset,
)

# analysis = test_basic_path()
analysis = test_dp_mean()
# analysis = test_raw_dataset()

analysis.plot()

print('analysis is valid:', analysis.validate())

print('epsilon:', analysis.epsilon)
#
print('release json:', analysis.release())
# print('release proto:', analysis.release_proto)
