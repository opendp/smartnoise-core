import yarrow

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'


def test_basic_path():
    print('file path test')

    with yarrow.Analysis() as analysis:
        PUMS = yarrow.Dataset('PUMS', test_csv_path)

        age = PUMS[('age', int)]
        sex = PUMS[('sex', int)]

        mean_age = yarrow.ops.dp_mean(
            PUMS[('married', float)],
            epsilon=.065,
            minimum=0,
            maximum=100,
            n=500
        )

        yarrow.ops.dp_mean(
            age / 2 + (sex + 22),
            epsilon=.1,
            minimum=mean_age - 5.2,
            maximum=102,
            n=500) + 5.

        yarrow.ops.dp_variance(
            PUMS[('educ', int)],
            epsilon=.15,
            n=1000,
            minimum=0,
            maximum=12
        )

        yarrow.ops.dp_moment_raw(
            PUMS[('married', float)],
            epsilon=.15,
            n=1000000,
            minimum=0,
            maximum=12,
            order=3
        )

        yarrow.ops.dp_covariance(
            PUMS[('sex', int)],
            PUMS[('married', int)],
            epsilon=.15,
            n=1000,
            minimum_left=0,
            maximum_left=1,
            minimum_right=0,
            maximum_right=1
        )

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    analysis.plot()

    print('release json:', analysis.release())
    print('release proto:', analysis.release_proto)


def test_raw_dataset():
    with yarrow.Analysis() as analysis:
        yarrow.ops.dp_mean(
            data=[1., 2., 3., 4., 5.],
            epsilon=1,
            minimum=0,
            maximum=10,
            n=10
        )

    analysis.plot()
    print('release json:', analysis.release())
    print('release proto:', analysis.release_proto)
