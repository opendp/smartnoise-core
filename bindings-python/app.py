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

print('epsilon:', analysis.epsilon)

release_report = analysis.release()
print('release json:', json.dumps(release_report, indent=4))
print('all released values:', analysis.release_values)
