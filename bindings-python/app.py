import json

from tests.test_base import (
    test_dp_linear_stats,
    test_integers,
    test_multilayer_analysis,
    test_raw_dataset
)

# turn on stack traces
import os

os.environ['RUST_BACKTRACE'] = 'full'

# analysis = test_multilayer_analysis(run=False)
# analysis = test_dp_linear_stats(run=False)
# analysis = test_raw_dataset(run=False)
test_integers()

# analysis.plot()
# print(analysis.release_values)
# print(analysis.components[13].get_usages())
# print('privacy usage:', analysis.privacy_usage)
#
# analysis.release()
#
# print('all released values (internal):', analysis.release_values)
# print('release report:', json.dumps(analysis.report(), indent=4))
