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
    with whitenoise.Analysis(eager=True) as analysis:
        PUMS = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        age = op.cast(PUMS['age'], type="FLOAT")
        sex = op.cast(PUMS['sex'], type="BOOL", true_label="TRUE")

        age_clamped = op.clamp(age, lower=0., upper=150.)
        age_resized = op.resize(age_clamped, n=1000)

        mean_age = op.dp_mean(
            data=op.cast(PUMS['race'], type="FLOAT"),
            privacy_usage={'epsilon': .65},
            data_lower=0.,
            data_upper=100.,
            data_n=500
        )
        analysis.release()

        sex_plus_22 = op.add(
            op.cast(sex, type="FLOAT"),
            22.,
            left_n=1000, left_lower=0., left_upper=1.)

        op.dp_mean(
            age_resized / 2. + sex_plus_22,
            privacy_usage={'epsilon': .1},
            data_lower=mean_age - 5.2,
            data_upper=102.,
            data_n=500) + 5.

        op.dp_variance(
            op.cast(PUMS['educ'], type="FLOAT"),
            privacy_usage={'epsilon': .15},
            data_n=1000,
            data_lower=0.,
            data_upper=12.
        )

        # op.dp_moment_raw(
        #     op.cast(PUMS['married'], type="FLOAT"),
        #     privacy_usage={'epsilon': .15},
        #     data_n=1000000,
        #     data_lower=0.,
        #     data_upper=12.,
        #     order=3
        # )
        #
        # op.dp_covariance(
        #     left=op.cast(PUMS['age'], type="FLOAT"),
        #     right=op.cast(PUMS['married'], type="FLOAT"),
        #     privacy_usage={'epsilon': .15},
        #     left_n=1000,
        #     right_n=1000,
        #     left_lower=0.,
        #     left_upper=1.,
        #     right_lower=0.,
        #     right_upper=1.
        # )

    if run:
        analysis.release()

    return analysis


def test_dp_linear_stats(run=True):
    with whitenoise.Analysis(filter_level='public_and_prior') as analysis:
        dataset_pums = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        age = dataset_pums['age']
        analysis.release()

        num_records = op.dp_count(
            age,
            privacy_usage={'epsilon': .5},
            lower=0,
            upper=10000
        )
        analysis.release()

        print("number of records:", num_records.value)

        vars = op.cast(dataset_pums[["age", "income"]], type="float")

        covariance = op.dp_covariance(
            data=vars,
            privacy_usage={'epsilon': .5},
            data_lower=[0., 0.],
            data_upper=[150., 150000.],
            data_n=num_records)
        analysis.release()
        print("covariance released")

        num_means = op.dp_mean(
            data=vars,
            privacy_usage={'epsilon': .5},
            data_lower=[0., 0.],
            data_upper=[150., 150000.],
            data_n=num_records)

        analysis.release()
        print("covariance:\n", covariance.value)
        print("means:\n", num_means.value)

        age = op.cast(age, type="FLOAT")

        age_variance = op.dp_variance(
            age,
            privacy_usage={'epsilon': .5},
            data_lower=0.,
            data_upper=150.,
            data_n=num_records)

        analysis.release()

        print("age variance:", age_variance.value)

        # If I clamp, impute, resize, then I can reuse their properties for multiple statistics
        clamped_age = op.clamp(age, lower=0., upper=100.)
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

        # mean with lower, upper properties propagated up from prior bounds
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

        custom_maximum = op.laplace_mechanism(
            op.maximum(preprocessed_age),
            privacy_usage={'epsilon': .5})

        custom_maximum = op.laplace_mechanism(
            op.maximum(preprocessed_age),
            privacy_usage={'epsilon': .5})

        custom_quantile = op.laplace_mechanism(
            op.quantile(preprocessed_age, quantile=.5),
            privacy_usage={'epsilon': 500})

        income = op.cast(dataset_pums['income'], type="FLOAT")
        income_max = op.laplace_mechanism(
            op.maximum(income, data_lower=0., data_upper=1000000.),
            privacy_usage={'epsilon': 10})

        # releases may also be postprocessed and reused as arguments to more components
        age_sum + custom_maximum * 23.

        analysis.release()
        print("laplace quantile:", custom_quantile.value)

        age_histogram = op.dp_histogram(
            op.cast(age, type='int', lower=0, upper=100),
            edges=list(range(0, 100, 25)),
            count_upper=300,
            null_value=150,
            privacy_usage={'epsilon': 2.}
        )

        sex_histogram = op.dp_histogram(
            op.cast(dataset_pums['sex'], type='bool', true_label="1"),
            count_upper=1000,
            privacy_usage={'epsilon': 2.}
        )

        education_histogram = op.dp_histogram(
            dataset_pums['educ'],
            categories=["5", "7", "10"],
            null_value="-1",
            privacy_usage={'epsilon': 2.}
        )

        analysis.release()

        print("age histogram: ", age_histogram.value)
        print("sex histogram: ", sex_histogram.value)
        print("education histogram: ", education_histogram.value)

    if run:
        analysis.release()

        # get the mean computed when release() was called
        print(mean.value)
        print(variance.value)

    return analysis


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


def test_raw_dataset(run=True):
    with whitenoise.Analysis() as analysis:
        op.dp_mean(
            data=whitenoise.Dataset(value=[1., 2., 3., 4., 5.], num_columns=1),
            privacy_usage={'epsilon': 1},
            data_lower=0.,
            data_upper=10.,
            data_n=10,
        )

    if run:
        analysis.release()

    return analysis


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
        race_histogram = op.histogram(race, categories=["1", "2", "3"], null_value="3")
        op.histogram(income, edges=[0., 10000., 50000.], null_value=-1)

        op.dp_histogram(married, privacy_usage={"epsilon": 0.5})

        op.gaussian_mechanism(race_histogram, privacy_usage={"epsilon": 0.5, "delta": .000001})
        op.laplace_mechanism(race_histogram, privacy_usage={"epsilon": 0.5, "delta": .000001})

        op.kth_raw_sample_moment(educ, k=3)

        op.log(op.clamp(educ, 0.001, 50.))
        op.maximum(educ)
        op.mean(educ)
        op.minimum(educ)

        educ % 2.
        educ ** 2.

        op.quantile(educ, .32)

        op.resize(educ, 1200, 0., 50.)
        op.resize(race, 1200, categories=["1", "2"], weights=[1, 2])
        op.resize(data[["age", "sex"]], 1200, categories=[["1", "2"], ["a", "b"]], weights=[1, 2])
        op.resize(
            data[["age", "sex"]], 1200,
            categories=[["1", "2"], ["a", "b", "c"]],
            weights=[[1, 2], [3, 7, 2]])

        op.sum(educ)
        op.variance(educ)

    if run:
        analysis.release()

    return analysis


def test_histogram():
    import whitenoise
    import whitenoise.components as op
    import numpy as np

    # establish data information

    data = np.genfromtxt(TEST_CSV_PATH, delimiter=',', names=True)
    education_categories = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17"]

    income = list(data[:]['income'])
    income_edges = list(range(0, 100_000, 10_000))

    print('actual', np.histogram(income, bins=income_edges)[0])

    with whitenoise.Analysis() as analysis:
        data = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)
        income = op.to_int(data['income'], lower=0, upper=0)
        sex = op.to_bool(data['sex'], true_label="1")

        income_histogram = op.dp_histogram(
            income,
            edges=income_edges,
            privacy_usage={'epsilon': 1.}
        )

    analysis.release()

    print("Income histogram Geometric DP release:   " + str(income_histogram.value))


def test_covariance():
    import whitenoise
    import whitenoise.components as op
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt

    data = np.genfromtxt(TEST_CSV_PATH, delimiter=',', names=True)

    with whitenoise.Analysis() as analysis:
        wn_data = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)
        # get full covariance matrix
        cov = op.dp_covariance(data=op.to_float(wn_data['age', 'sex', 'educ', 'income', 'married']),
                               privacy_usage={'epsilon': 10},
                               data_lower=[0., 0., 1., 0., 0.],
                               data_upper=[100., 1., 16., 500_000., 1.],
                               data_n=1000)
    analysis.release()

    # store DP covariance and correlation matrix
    dp_cov = cov.value
    dp_corr = dp_cov / np.outer(np.sqrt(np.diag(dp_cov)), np.sqrt(np.diag(dp_cov)))

    # get non-DP covariance/correlation matrices
    age = list(data[:]['age'])
    sex = list(data[:]['sex'])
    educ = list(data[:]['educ'])
    income = list(data[:]['income'])
    married = list(data[:]['married'])
    non_dp_cov = np.cov([age, sex, educ, income, married])
    non_dp_corr = non_dp_cov / np.outer(np.sqrt(np.diag(non_dp_cov)), np.sqrt(np.diag(non_dp_cov)))

    print('Non-DP Covariance Matrix:\n{0}\n\n'.format(pd.DataFrame(non_dp_cov)))
    print('Non-DP Correlation Matrix:\n{0}\n\n'.format(pd.DataFrame(non_dp_corr)))
    print('DP Correlation Matrix:\n{0}'.format(pd.DataFrame(dp_corr)))
    plt.imshow(non_dp_corr - dp_corr, interpolation='nearest')
    plt.colorbar()
    plt.show()


def test_properties():
    with whitenoise.Analysis():
        # load data
        data = whitenoise.Dataset(path=TEST_CSV_PATH, column_names=test_csv_names)

        # establish data
        age_dt = op.cast(data['age'], 'FLOAT')

        # ensure data are non-null
        non_null_age_dt = op.impute(age_dt, distribution='Uniform', lower=0., upper=100.)
        clamped = op.clamp(age_dt, lower=0., upper=100.)

        # create potential for null data again
        potentially_null_age_dt = non_null_age_dt / 0.

        # print('original properties:\n{0}\n\n'.format(age_dt.properties))
        print('properties after imputation:\n{0}\n\n'.format(non_null_age_dt.nullity))
        print('properties after nan mult:\n{0}\n\n'.format(potentially_null_age_dt.nullity))

        print("lower", clamped.lower)
        print("upper", clamped.upper)
        print("releasable", clamped.releasable)
        # print("props", clamped.properties)
        print("data_type", clamped.data_type)
        print("categories", clamped.categories)
