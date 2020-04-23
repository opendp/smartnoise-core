import json

from tests.test_base import (
    test_dp_linear_stats,
    test_multilayer_analysis,
    test_raw_dataset,
    test_everything,
    test_histogram,
    test_covariance,
    test_properties
)

from tests import test_validator_properties
from tests import test_insertion

# turn on stack traces from panics
import os
os.environ['RUST_BACKTRACE'] = 'full'

test_insertion.test_insertion_simple()
test_validator_properties.test_partition()

test_properties()


analysis = test_multilayer_analysis(run=False)
analysis.release()

analysis = test_dp_linear_stats(run=False)
analysis.release()

analysis = test_raw_dataset(run=False)
analysis.release()

analysis = test_everything(run=False)
analysis.release()

analysis.plot()
analysis.clean()
print('privacy usage:', analysis.privacy_usage)

# print('all released values (internal):', analysis.release_values)
print('release report:', json.dumps(analysis.report(), indent=4))

test_histogram()
test_covariance()
