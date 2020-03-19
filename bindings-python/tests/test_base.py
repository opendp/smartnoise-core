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

def test_multilayer_analysis(run=True):

    with whitenoise.Analysis() as analysis:
        PUMS = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        age = op.cast(PUMS['age'], type="FLOAT")
        sex = op.cast(PUMS['sex'], type="BOOL", true_label="TRUE")

        age_clamped = op.clamp(age, min=0., max=150.)
        age_resized = op.resize(age_clamped, n=1000)

        mean_age = op.dp_mean(
            data=op.cast(PUMS['married'], type="FLOAT"),
            privacy_usage={'epsilon': .65},
            data_min=0.,
            data_max=100.,
            data_n=500
        )

        analysis.release()

        sex_plus_22 = op.add(
            op.cast(sex, type="FLOAT"),
            22.,
            left_n=1000, left_min=0., left_max=1.)

        op.dp_mean(
            age_resized / 2. + sex_plus_22,
            privacy_usage={'epsilon': .1},
            data_min=mean_age - 5.2,
            data_max=102.,
            data_n=500) + 5.

        op.dp_variance(
            op.cast(PUMS['educ'], type="FLOAT"),
            privacy_usage={'epsilon': .15},
            data_n=1000,
            data_min=0.,
            data_max=12.
        )

        # op.dp_moment_raw(
        #     op.cast(PUMS['married'], type="FLOAT"),
        #     privacy_usage={'epsilon': .15},
        #     data_n=1000000,
        #     data_min=0.,
        #     data_max=12.,
        #     order=3
        # )
        #
        # op.dp_covariance(
        #     left=op.cast(PUMS['age'], type="FLOAT"),
        #     right=op.cast(PUMS['married'], type="FLOAT"),
        #     privacy_usage={'epsilon': .15},
        #     left_n=1000,
        #     right_n=1000,
        #     left_min=0.,
        #     left_max=1.,
        #     right_min=0.,
        #     right_max=1.
        # )

    if run:
        analysis.release()

    return analysis


def test_dp_linear_stats(run=True):
    with whitenoise.Analysis() as analysis:
        dataset_pums = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        age = dataset_pums['age']
        analysis.release()

        num_records = op.dp_count(
            age,
            privacy_usage={'epsilon': .5},
            count_min=0,
            count_max=10000
        )
        analysis.release()
        print("number of records:", num_records.value)

        age = op.cast(age, type="FLOAT")

        age_variance = op.dp_variance(
            age,
            privacy_usage={'epsilon': .5},
            data_min=0.,
            data_max=150.,
            data_n=num_records)

        analysis.release()
        print("age variance:", age_variance.value)

        # If I clamp, impute, resize, then I can reuse their properties for multiple statistics
        clamped_age = op.clamp(age, min=0., max=100.)
        imputed_age = op.impute(clamped_age)
        preprocessed_age = op.resize(imputed_age, n=num_records)

        # properties necessary for mean are statically known
        mean = op.dp_mean(
            preprocessed_age,
            privacy_usage={'epsilon': .5}
        )

        # properties necessary for variance are statically known
        variance = op.dp_variance(
            preprocessed_age,
            privacy_usage={'epsilon': .5}
        )

        # sum doesn't need n, so I pass the data in before resizing
        age_sum = op.dp_sum(
            imputed_age,
            privacy_usage={'epsilon': .5}
        )

        # mean with min, max properties propagated up from prior bounds
        transformed_mean = op.dp_mean(
            -(preprocessed_age + 2.),
            privacy_usage={'epsilon': .5}
        )

        analysis.release()
        print("age transformed mean:", transformed_mean.value)

        # releases may be pieced together from combinations of smaller components
        custom_mean = op.laplace_mechanism(
            op.mean(preprocessed_age),
            privacy_usage={'epsilon': .5})

        custom_minimum = op.laplace_mechanism(
            op.minimum(preprocessed_age),
            privacy_usage={'epsilon': .5})

        custom_maximum = op.laplace_mechanism(
            op.maximum(preprocessed_age),
            privacy_usage={'epsilon': .5})

        custom_quantile = op.laplace_mechanism(
            op.quantile(preprocessed_age, quantile=.5),
            privacy_usage={'epsilon': 500})

        income = op.cast(dataset_pums['income'], type="FLOAT")
        income_max = op.laplace_mechanism(
            op.maximum(income, data_min=0., data_max=1000000.),
            privacy_usage={'epsilon': 10})

        # releases may also be postprocessed and reused as arguments to more components
        age_sum + custom_minimum * 23.

        analysis.release()
        print("laplace quantile:", custom_quantile.value)


    if run:
        analysis.release()

        # get the mean computed when release() was called
        print(mean.value)
        print(variance.value)

    return analysis

@pytest.mark.skip(reason="requires count_min and count_max")
def test_dp_count(run=True):
    with whitenoise.Analysis() as analysis:
        dataset_pums = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        count = op.dp_count(
            dataset_pums['sex'] == '1',
            privacy_usage={'epsilon': 0.5})

    if run:
        analysis.release()
        print(count.value)

    return analysis


@pytest.mark.skip(reason="Needs num_columns or column_names")
def test_raw_dataset(run=True):
    with whitenoise.Analysis() as analysis:
        op.dp_mean(
            data=whitenoise.Dataset(value=[1., 2., 3., 4., 5.])[0],
            privacy_usage={'epsilon': 1},
            data_min=0.,
            data_max=10.,
            data_n=10,
        )

    if run:
        analysis.release()

    return analysis
