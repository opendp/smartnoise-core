import burdock

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'


def test_basic_path():
    print('file path test')

    with burdock.Analysis() as analysis:
        PUMS = burdock.Dataset('PUMS', test_csv_path)

        age = PUMS[('age', int)]
        sex = PUMS[('sex', int)]

        mean_age = burdock.dp_mean_laplace(
            PUMS[('race', float)],
            epsilon=.065,
            minimum=0,
            maximum=100,
            num_records=500
        )

        burdock.dp_mean_laplace(
            age / 2 + (sex + 22),
            epsilon=.1,
            minimum=mean_age - 5.2,
            maximum=102,
            num_records=500)

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    analysis.plot()

    print('release json:', analysis.release())
    print('release proto:', analysis.release_proto)


def test_haskell_validator():

    import ctypes
    import analysis_pb2
    haskell_path = f"../validator-haskell/.stack-work/install/x86_64-linux/" \
                   "148d0e92cd3f02b3b71e5e570acc02f4fd5aeac7a29166dac7a6b62c52d8796b/" \
                   "8.6.5/lib/{prefix}Validator{extension}"
    validator_lib = ctypes.cdll.LoadLibrary(haskell_path)
    validator_lib.getProto.restype = ctypes.c_char_p

    validator_lib.DPValidatorInit()
    validator_lib.showProtos()

    buffer = validator_lib.getProto()
    print("buffer:", buffer)

    print(analysis_pb2.Component.FromString(buffer))

    validator_lib.validate_analysis(buffer)


def test_rust_sampling():
    import ctypes
    import matplotlib.pyplot as plt

    n_samples = 10000
    buffer = (ctypes.c_double * n_samples)(*(0 for _ in range(n_samples)))

    burdock.core_wrapper.lib_runtime.test_sample_uniform(buffer, n_samples)
    plt.hist(list(buffer))
    plt.title("uniform samples")
    plt.show()

    burdock.core_wrapper.lib_runtime.test_sample_laplace(buffer, n_samples)
    plt.hist(list(buffer))
    plt.title("laplace samples")
    plt.show()


def test_ndarray():

    burdock.core_wrapper.lib_runtime.test_ndarray()