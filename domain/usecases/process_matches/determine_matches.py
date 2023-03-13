import pandas as pd
import time
import os 
from os import listdir
from os.path import isfile, join
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.cosine import Cosine
from strsimpy.jaccard import Jaccard
from strsimpy.sorensen_dice import SorensenDice
from domain.usecases.algorithms.calculate_algorithms import CalculateAlgorithms
from fuzzywuzzy import fuzz
from tqdm import tqdm

jaro_winkler = JaroWinkler()
normalized_levenshetein = NormalizedLevenshtein()
cosine = Cosine(1)
ji = Jaccard(1)
sd = SorensenDice()

JW = lambda x, y: jaro_winkler.similarity(x, y)
NL = lambda x, y: normalized_levenshetein.similarity(x, y)
COS = lambda x, y: cosine.similarity(x, y)
JC = lambda x, y: ji.similarity(x, y)
SD = lambda x, y: sd.similarity(x, y)
FW = lambda x, y: fuzz.ratio(x, y) / 100

algorithms = [JW, NL, COS, JC, SD, FW]
algorithms_names = ["11", "12", "13", "14", "15", "16"]
threshold = 60 
path_sample = os.path.join('data/muestra.csv')
path_vinculos = os.path.join('data/vinculos')
path_files = [join(path_vinculos,f) for f in listdir(path_vinculos) if isfile(join(path_vinculos, f))]
path_rv = os.path.join('data/ResultadosVinculos.csv')
path_el = os.path.join('data/EntradasListas.csv')


cols = ["idSolicitud", "idNumber", "IdEntrada", "Nombre_EL", "Nombre", "Nombre1", "Nombre2", "Apellido1",
        "Apellido2", "algorithm", "criterio", "Resultado"]

class DetermineMatches:
    def __init__(self):
        pass


    @staticmethod
    def exec(algorithm_index):
        if (algorithm_index > 0) & (algorithm_index < 7):
            position = algorithm_index -1
            alg = algorithms[position]
            name = algorithms_names[position]
            print("Starting process")
            for path_iter in path_files:
                CalculateAlgorithms.iter_sample(path_iter,path_rv,path_el,cols,alg,name,threshold)            
        else: 
            print("Ingresar un valor entre 1 y 6.")
         

    @staticmethod
    def exec_vinculo(item, cols: list):
        row = pd.Series({"Nombre": item.Nombre, "Nombre1": item.Nombre1, "Nombre2": item.Nombre2,
                         "Apellido1": item.Apellido1, "Apellido2": item.Apellido2},
                        index=["Nombre", "Nombre1", "Nombre2", "Apellido1", "Apellido2"])
        alg = item.algorithm + 11
        algorithm = algorithms[item.algorithm]
        inputs = pd.read_csv('data/EntradasListas.csv', chunksize=100000)
        init = time.time()
        for chunk in tqdm(inputs):
            chunk["Nombre1"] = chunk["Nombre1"].astype(str)
            result = CalculateAlgorithms.find_matches(chunk, row, algorithm, algorithms_name, threshold, item.algorithm)
            if result.shape[0] > 1:
               result[cols].to_csv('data/ResultadosVinculos{}.csv'.format(alg), index=False ,mode='a', header=False)
        duration = time.time() - init        
        return duration

    
    @classmethod
    def iter_string(cls, item):
        alg = item.algorithm + 11
        df = pd.DataFrame(columns=cols)
        df.to_csv('data/ResultadosVinculos{}.csv'.format(alg), index=False)
        duration = cls.exec_vinculo(item, cols)
        return duration
