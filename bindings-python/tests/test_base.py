import burdock
import pandas as pd

test_csv_path = '/home/shoe/PSI/datasets/data/PUMS_california_demographics_1000/data.csv'

def test_basic_data():

    with burdock.Analysis() as analysis:
        age = burdock.Component('DataSource', options={"dataset_id": "PUMS", "column_id": "age"})

        # TODO: automatic conversion of elementary types to Literals or Constants
        two = burdock.Component('Literal', options={"numeric": 2})

        burdock.dp_mean_laplace(age + two, epsilon=.1)

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    # ALERT: data is not inside repository
    data = pd.read_csv(test_csv_path)[['age', 'income']]

    print('release json:', analysis.release(data))
    print('release proto:', analysis.release_proto)


def test_basic_path():

    with burdock.Analysis() as analysis:
        age = burdock.Component('DataSource', options={"column_id": "age"})
        sex = burdock.Component('DataSource', options={"column_id": "sex"})

        burdock.dp_mean_laplace(age + sex, epsilon=.1, minimum=2, maximum=102, num_records=500)

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    analysis.plot()

    print('release json:', analysis.release(test_csv_path))
    print('release proto:', analysis.release_proto)


def test_haskell_validator():

    import ctypes
    import analysis_pb2
    haskell_path = "../validator-haskell/.stack-work/install/x86_64-linux/" \
                   "148d0e92cd3f02b3b71e5e570acc02f4fd5aeac7a29166dac7a6b62c52d8796b/" \
                   "8.6.5/lib/libValidator.so"
    validator_lib = ctypes.cdll.LoadLibrary(haskell_path)
    validator_lib.getProto.restype = ctypes.c_char_p

    validator_lib.DPValidatorInit()

    print(validator_lib.foo(7))
    validator_lib.showProtos()

    buffer = validator_lib.getProto()
    print("buffer:", buffer)

    print(analysis_pb2.Component.FromString(buffer))
