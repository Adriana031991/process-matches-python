from calculate_algorithms import CalculateAlgorithms
import pandas as pd

route = 'D:\dev\cadena\process-matches\data\muestra.csv'


def test_combinations():
    data = CalculateAlgorithms.create_combinations(pd.read_csv(route))

    assert type(data) == pd.core.frame.DataFrame
