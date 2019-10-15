import burdock
import pandas as pd

test_csv_path = '/home/shoe/PSI/data/California Demographic Dataset.csv'


def test_basic():

    with burdock.Analysis() as analysis:
        age = burdock.Component('DataSource', options={"dataset_id": "PUMS", "column_id": "age"})

        # TODO: automatic conversion of elementary types to Literals or Constants
        two = burdock.Component('Literal', options={"numeric": 2})

        burdock.mean(age + two)

    print('analysis is valid:', analysis.validate())

    print('epsilon:', analysis.epsilon)

    # ALERT: data is not inside repository
    data = pd.read_csv(test_csv_path)[['age', 'income']]

    print('release json:', analysis.release(data))
    print('release proto:', analysis.release_proto)
