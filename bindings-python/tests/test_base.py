import yarrow

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'


def test_basic_path():
    print('file path test')

    with yarrow.Analysis() as analysis:
        PUMS = yarrow.Dataset('PUMS', test_csv_path)

        age = PUMS[('age', int)]
        sex = PUMS[('sex', int)]

        mean_age = yarrow.ops.dp_mean(
            data=PUMS[('married', float)],
            privacy_usage=yarrow.privacy_usage(epsilon=.65),
            data_min=0.,
            data_max=100.,
            data_n=500
        )

        yarrow.ops.dp_mean(
            age / 2 + (sex + 22),
            privacy_usage=yarrow.privacy_usage(epsilon=.1),
            data_min=mean_age - 5.2,
            data_max=102.,
            data_n=500) + 5.

        yarrow.ops.dp_variance(
            PUMS[('educ', int)],
            privacy_usage=yarrow.privacy_usage(epsilon=.15),
            data_n=1000,
            data_min=0.,
            data_max=12.
        )

        yarrow.ops.dp_moment_raw(
            PUMS[('married', float)],
            privacy_usage=yarrow.privacy_usage(epsilon=.15),
            data_n=1000000,
            data_min=0.,
            data_max=12.,
            order=3
        )

        yarrow.ops.dp_covariance(
            PUMS[('sex', int)],
            PUMS[('married', int)],
            privacy_usage=yarrow.privacy_usage(epsilon=.15),
            left_n=1000,
            right_n=1000,
            left_min=0.,
            left_max=1.,
            right_min=0.,
            right_max=1.
        )

    analysis.release()
    return analysis


def test_raw_dataset():
    with yarrow.Analysis() as analysis:
        yarrow.ops.dp_mean(
            data=[1., 2., 3., 4., 5.],
            privacy_usage=yarrow.privacy_usage(epsilon=1),
            data_min=0.,
            data_max=10.,
            data_n=10
        )
    analysis.release()

    return analysis
