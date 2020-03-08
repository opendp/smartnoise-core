import yarrow

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'


def test_multilayer_analysis(run=True):

    with yarrow.Analysis() as analysis:
        PUMS = yarrow.Dataset(path=test_csv_path)

        age = yarrow.ops.cast(PUMS['age'], type="FLOAT")
        sex = yarrow.ops.cast(PUMS['sex'], type="BOOL", positive="TRUE")

        mean_age = yarrow.ops.dp_mean(
            data=yarrow.ops.cast(PUMS['married'], type="FLOAT"),
            privacy_usage={'epsilon': .65},
            data_min=0.,
            data_max=100.,
            data_n=500
        )

        yarrow.ops.dp_mean(
            age / 2 + (sex + 22),
            privacy_usage={'epsilon': .1},
            data_min=mean_age - 5.2,
            data_max=102.,
            data_n=500) + 5.

        yarrow.ops.dp_variance(
            yarrow.ops.cast(PUMS['educ'], type="FLOAT"),
            privacy_usage={'epsilon': .15},
            data_n=1000,
            data_min=0.,
            data_max=12.
        )

        yarrow.ops.dp_moment_raw(
            yarrow.ops.cast(PUMS['married'], type="FLOAT"),
            privacy_usage={'epsilon': .15},
            data_n=1000000,
            data_min=0.,
            data_max=12.,
            order=3
        )

        yarrow.ops.dp_covariance(
            yarrow.ops.cast(PUMS['age'], type="FLOAT"),
            yarrow.ops.cast(PUMS['married'], type="FLOAT"),
            privacy_usage={'epsilon': .15},
            left_n=1000,
            right_n=1000,
            left_min=0.,
            left_max=1.,
            right_min=0.,
            right_max=1.
        )

    if run:
        analysis.release()

    return analysis


def test_dp_linear_stats(run=True):
    with yarrow.Analysis() as analysis:
        dataset_pums = yarrow.Dataset(path=test_csv_path)

        age = dataset_pums['age']
        age = yarrow.ops.cast(age, type="FLOAT")

        clamped = yarrow.ops.clamp(age, min=0., max=100.)
        imputed = yarrow.ops.impute(clamped)
        resized = yarrow.ops.resize(imputed, n=500)

        mean = yarrow.ops.dp_mean(
            resized,
            privacy_usage={'epsilon': .5}
        )

        variance = yarrow.ops.dp_variance(
            resized,
            privacy_usage={'epsilon': .5}
        )

        sum = yarrow.ops.dp_sum(
            imputed,
            privacy_usage={'epsilon': .5}
        )

        # yarrow.ops.dp_covariance(
        #     privacy_usage={'epsilon': .5},
        #     left=resized,
        #     right=yarrow.ops.cast(dataset_pums['income'], type="FLOAT"),
        #     right_min=0.,
        #     right_max=1.,
        #     right_n=500
        # )

        yarrow.ops.dp_count(
            age,
            privacy_usage={'epsilon': .5},
            count_min=0,
            count_max=10000
        )

    if run:
        analysis.release()

        # get the mean computed when release() was called
        print(mean.value)
        print(variance.value)

    return analysis


def test_dp_count(run=True):
    with yarrow.Analysis() as analysis:
        dataset_pums = yarrow.Dataset(path=test_csv_path)
        count = yarrow.ops.dp_count(
            dataset_pums['sex'] == '1',
            privacy_usage={'epsilon': 0.5})

    if run:
        analysis.release()
        print(count.value)

    return analysis


def test_raw_dataset(run=True):
    with yarrow.Analysis() as analysis:
        yarrow.ops.dp_mean(
            data=yarrow.Dataset(value=[1., 2., 3., 4., 5.])[0],
            privacy_usage={'epsilon': 1},
            data_min=0.,
            data_max=10.,
            data_n=10
        )

    if run:
        analysis.release()

    return analysis
