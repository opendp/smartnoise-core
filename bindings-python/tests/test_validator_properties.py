import opendp.whitenoise as whitenoise
import opendp.whitenoise.components as op
import random
import string
import numpy as np

dataset_bools = {
    'value': [[True, True], [True, False], [False, True], [False, False]],
    'num_columns': 2
}


def generate_synthetic(var_type, n=10, rand_min=0, rand_max=10, cats_str=None, cats_num=None, variants=None):

    cats_str = ['A', 'B', 'C', 'D'] if cats_str is None else cats_str
    cats_num = [0, 1, 2, 3] if cats_num is None else cats_num
    variants = ['Index', 'Random', 'Constant', 'Categories'] if variants is None else variants

    data = []
    names = []

    for variant in variants:
        if var_type == bool:
            data.append(list({
                                 'Index': (bool(i % 2) for i in range(n)),
                                 'Random': (random.choice([True, False]) for _ in range(n)),
                                 'Constant': (bool(1) for _ in range(n)),
                                 'Categories': (bool(random.choice(cats_num)) for _ in range(n))
                             }[variant]))
            names.append('B_' + variant)
        if var_type == float:
            data.append(list({
                                 'Index': (float(i) for i in range(n)),
                                 'Random': (rand_min + random.random() * (rand_max - rand_min) for _ in range(n)),
                                 'Constant': (float(1) for _ in range(n)),
                                 'Categories': (float(random.choice(cats_num)) for _ in range(n)),
                             }[variant]))
            names.append('F_' + variant)
        if var_type == int:
            data.append(list({
                                 'Index': range(n),
                                 'Random': (random.randrange(rand_min, rand_max) for _ in range(n)),
                                 'Constant': (1 for _ in range(n)),
                                 'Categories': (random.choice(cats_num) for _ in range(n)),
                             }[variant]))
            names.append('I_' + variant)
        if var_type == str:
            data.append(list({
                                 'Index': (str(i) for i in range(n)),
                                 'Random': (''.join([random.choice(string.ascii_letters + string.digits)
                                                     for n in range(2)]) for _ in range(n)),
                                 'Constant': (str(1) for _ in range(n)),
                                 'Categories': (random.choice(cats_str) for _ in range(n)),
                             }[variant]))
            names.append('S_' + variant)

    return {'value': list(zip(*data)), 'column_names': names}


def test_divide():
    with whitenoise.Analysis():
        data_A = whitenoise.Dataset(**generate_synthetic(float, variants=['Random']))

        f_random = data_A['F_Random']
        imputed = op.impute(f_random, lower=0., upper=10.)
        clamped_nonzero = op.clamp(imputed, lower=1., upper=10.)
        clamped_zero = op.clamp(imputed, lower=0., upper=10.)

        # test properties
        assert f_random.nullity
        assert not imputed.nullity
        assert (2. / imputed).nullity
        assert (f_random / imputed).nullity
        assert (2. / clamped_zero).nullity

        # TODO: fix these assertions in the validator- we should be able to get these tighter bounds
        # assert not (2. / clamped_nonzero).nullity
        # assert not (imputed / 2.).nullity


def test_dp_mean():
    with whitenoise.Analysis():
        data = whitenoise.Dataset(**generate_synthetic(float, variants=['Random']))
        mean = op.dp_mean(
            data[['F_Random', 'F_Random']],
            accuracy={'value': .2, 'alpha': .05},
            data_lower=[0., 5.],
            data_upper=10.,
            data_n=10)

        print("accuracy", mean.get_accuracy(0.05))
        print(mean.from_accuracy(2.3, .05))


def test_equal():
    with whitenoise.Analysis(filter_level='all') as analysis:
        data = whitenoise.Dataset(**dataset_bools)

        equality = data[0] == data[1]

        analysis.release()
        assert np.array_equal(equality.value, np.array([True, False, False, True]))


def test_partition():
    with whitenoise.Analysis(filter_level='all') as analysis:
        data = whitenoise.Dataset(**dataset_bools)[[0, 1]]

        partitioned = op.partition(data, num_partitions=3)
        analysis.release()
        # print(partitioned.value)

        assert np.array_equal(partitioned.value[0], np.array([[True, True], [True, False]]))
        assert np.array_equal(partitioned.value[1], np.array([[False, True]]))
        assert np.array_equal(partitioned.value[2], np.array([[False, False]]))


def test_index():
    with whitenoise.Analysis(filter_level='all') as analysis:
        data = whitenoise.Dataset(**dataset_bools)

        index_0 = data[0]

        analysis.release()
        assert index_0.value == [True, True, False, False]

