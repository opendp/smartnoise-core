import json

from tests.test_base import (
    test_basic_path,
    test_dp_mean,
    test_raw_dataset
)


# analysis = test_basic_path(run=False)
analysis = test_dp_mean(run=False)
# analysis = test_raw_dataset(run=False)


print('analysis is valid:', analysis.validate())
print('privacy usage:', analysis.privacy_usage)

analysis.release()

print('all released values (internal):', json.dumps(analysis.release_values, indent=4))
print('release report:', json.dumps(analysis.report(), indent=4))
