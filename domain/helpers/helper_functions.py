import pandas as pd
import os


def read_data(path_sample, path_rv, path_el, cols):

    data_sample = pd.read_csv(path_sample)
    data_sample.fillna('', inplace=True)
    data_sample["duration"] = 0

    entradas_listas = pd.read_csv(path_el)
    entradas_listas["Nombre1"] = entradas_listas["Nombre1"].astype(str)
    
    df = pd.DataFrame(columns=cols)
    df.to_csv(path_rv, index=False)

    return data_sample, entradas_listas
