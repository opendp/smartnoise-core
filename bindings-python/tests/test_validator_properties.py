
import whitenoise
import whitenoise.components as op


def test_logicals(run=True):
    with whitenoise.Analysis(filter_level='all') as analysis:
        data = whitenoise.Dataset(value=[
            [True, True], [True, False], [False, True], [False, False]
        ], num_columns=2)

        col_1 = data[0]
        equality = data[0] == data[1]

        analysis.release()
        print(col_1.value)
        print(equality.value)

    if run:
        analysis.release()

    return analysis