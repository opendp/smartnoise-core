import yarrow

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'


def test_basic_path():
    print('file path test')

    with yarrow.Analysis() as analysis:
        PUMS = yarrow.Dataset('PUMS', test_csv_path)

        age = PUMS[('age', int)]
        sex = PUMS[('sex', int)]

        mean_age = yarrow.dp_mean(
            PUMS[('married', float)],
            epsilon=.065,
            minimum=0,
            maximum=100,
            num_records=500
        )

        yarrow.dp_mean(
            age / 2 + (sex + 22),
            epsilon=.1,
            minimum=mean_age - 5.2,
            maximum=102,
            num_records=500) + 5.

        yarrow.dp_variance(
            PUMS[('educ', int)],
            epsilon=.15,
            num_records=1000,
            minimum=0,
            maximum=12
        )

        yarrow.dp_moment_raw(
            PUMS[('married', float)],
            epsilon=.15,
            num_records=1000000,
            minimum=0,
            maximum=12,
            order=3
        )

        yarrow.dp_covariance(
            PUMS[('sex', int)],
            PUMS[('married', int)],
            epsilon=.15,
            num_records=1000,
            minimum_x=0,
            maximum_x=1,
            minimum_y=0,
            maximum_y=1
        )

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    analysis.plot()

    print('release json:', analysis.release())
    print('release proto:', analysis.release_proto)


def test_rust_sampling():
    import ctypes
    import matplotlib.pyplot as plt

    n_samples = 10000
    buffer = (ctypes.c_double * n_samples)(*(0 for _ in range(n_samples)))

    yarrow.core_wrapper.lib_runtime.test_sample_uniform(buffer, n_samples)
    plt.hist(list(buffer))
    plt.title("uniform samples")
    plt.show()

    yarrow.core_wrapper.lib_runtime.test_sample_laplace(buffer, n_samples)
    plt.hist(list(buffer))
    plt.title("laplace samples")
    plt.show()


def test_ndarray():
    yarrow.core_wrapper.lib_runtime.test_ndarray()
