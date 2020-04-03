from os.path import abspath, dirname, isfile, join
import pytest
import whitenoise
import whitenoise.components as op

# Path to the test csv file
#
TEST_CSV_PATH = join(dirname(abspath(__file__)), '..', 'data',
                     'PUMS_california_demographics_1000', 'data.csv')
assert isfile(TEST_CSV_PATH), f'Error: file not found: {TEST_CSV_PATH}'

test_csv_names = ["age", "sex", "educ", "race", "income", "married"]


def test_everything(run=True):

    with whitenoise.Analysis(dynamic=True) as analysis:
        data = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        age_int = op.to_int(data['age'], 0, 150)
        sex = op.to_bool(data['sex'], "1")
        educ = op.to_float(data['educ'])
        race = data['race']
        income = op.to_float(data['income'])
        married = op.to_bool(data['married'], "1")

        numerics = op.to_float(data[['age', 'income']])

        # broadcast scalar over 2d, broadcast scalar over 1d, columnar broadcasting, left and right mul
        numerics * 2. + 2. * educ

        # add different values for each column
        numerics + [[1., 2.]]

        # index into first column
        age = numerics[0]
        income = numerics[[False, True]]

        # boolean ops and broadcasting
        mask = sex & married | (~married ^ False) | (age > 50.) | (age_int == 25)

        # numerical clamping
        op.clamp(numerics, 0., [150., 150_000.])
        op.clamp(data['educ'], categories=[str(i) for i in range(8, 10)], null_value="-1")

        op.count(mask)
        op.covariance(age, income)
        op.digitize(educ, edges=[1., 3., 10.], null_value=-1)

        # checks for safety against division by zero
        income / 2.
        income / op.clamp(educ, 5., 20.)

        op.dp_count(data, privacy_usage={"epsilon": 0.5})
        op.dp_count(mask, privacy_usage={"epsilon": 0.5})

        op.dp_histogram(mask, privacy_usage={"epsilon": 0.5})
        age = op.impute(op.clamp(age, 0., 150.))
        op.dp_maximum(age, privacy_usage={"epsilon": 0.5})
        op.dp_minimum(age, privacy_usage={"epsilon": 0.5})
        op.dp_median(age, privacy_usage={"epsilon": 0.5})

        age_n = op.resize(age, n=800)
        op.dp_mean(age_n, privacy_usage={"epsilon": 0.5})
        op.dp_moment_raw(age_n, order=3, privacy_usage={"epsilon": 0.5})

        op.dp_sum(age, privacy_usage={"epsilon": 0.5})
        op.dp_variance(age_n, privacy_usage={"epsilon": 0.5})

        op.filter(income, mask)
        op.histogram(race, categories=["1", "2", "3"], null_value="3")

    analysis.release()
    print("b")
