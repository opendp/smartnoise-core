import json

from tests.test_base import (
    test_dp_linear_stats,
    test_multilayer_analysis,
    test_raw_dataset
)


# analysis = test_multilayer_analysis(run=False)
analysis = test_dp_linear_stats(run=False)
# analysis = test_raw_dataset(run=False)


analysis.plot()
print('analysis is valid:', analysis.validate())
print('privacy usage:', analysis.privacy_usage)

analysis.release()

print('all released values (internal):', json.dumps(analysis.release_values, indent=4))
print('release report:', json.dumps(analysis.report(), indent=4))
