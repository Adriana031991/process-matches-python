from fuzzywuzzy import fuzz
import numpy as np
import pandas as pd
import time
from domain.helpers.helper_functions import read_data
from tqdm import tqdm
from pandas.core.reshape.concat import concat
from infrastructure.external.storage_provider import StorageProvider
import mapply

mapply.init(n_workers=-1)
tqdm.pandas()

storage = StorageProvider("sofkai.appspot.com")

class CalculateAlgorithms:
    
    def __init__(self):
        pass


    @staticmethod
    def create_combinations(link_list):
        link_list['combination1'] = link_list['Nombre1'] + ' ' + link_list['Apellido1'] + ' ' + link_list['Apellido2']
        link_list['combination2'] = link_list['Nombre1'] + ' ' + link_list['Apellido2'] + ' ' + link_list['Apellido1']
        link_list['combination3'] = link_list['Apellido1'] + ' ' + link_list['Nombre1'] + ' ' + link_list['Apellido2']
        link_list['combination4'] = link_list['Apellido1'] + ' ' + link_list['Apellido2'] + ' ' + link_list['Nombre1']
        link_list['combination5'] = link_list['Apellido2'] + ' ' + link_list['Nombre1'] + ' ' + link_list['Apellido1']
        link_list['combination6'] = link_list['Apellido2'] + ' ' + link_list['Apellido1'] + ' ' + link_list['Nombre1']
        return link_list


    @classmethod
    def calculate_algorithms(cls, inputs, link_list, algorithm):
        inputs['Nombre1'] = inputs['Nombre1'].astype(str)
        inputs["id"] = link_list["id"]
        inputs["idSolicitud"] = link_list["idSolicitud"]
        inputs["idNumber"] = link_list["idNumber"]
        inputs["Nombre"] = link_list["Nombre"]
        inputs["Nombre1"] = link_list["Nombre1"]
        inputs["Nombre2"] = link_list["Nombre2"]
        inputs["Apellido1"] = link_list["Apellido1"]
        inputs["Apellido2"] = link_list["Apellido2"]
        inputs["Resultado"] = 0
        inputs["Resultado1"] = 0
        inputs["Resultado2"] = 0
        inputs["Resultado3"] = 0
        inputs["Resultado4"] = 0
        inputs["Resultado5"] = 0
        inputs["Resultado6"] = 0

        if link_list['Nombre1'] != '':
            link_list = cls.create_combinations(link_list)
            inputs["Resultado1"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion1']), axis=1) * 100
            inputs["Resultado2"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion2']), axis=1) * 100
            inputs["Resultado3"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion3']), axis=1) * 100
            inputs["Resultado4"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion4']), axis=1) * 100
            inputs["Resultado5"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion5']), axis=1) * 100
            inputs["Resultado6"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['combiantion6']), axis=1) * 100
        else:
            inputs["Resultado"] = inputs.apply(
                lambda x: algorithm(x['Nombre1'], link_list['Nombre']), axis=1) * 100
        result = inputs[ inputs.apply(lambda x: x[["Resultado", "Resultado1", "Resultado2", "Resultado3"]], axis=1)]


    @classmethod
    def prepare_link_list(cls, link_list):
        if link_list['Nombre1'] != '':
            link_list = cls.create_combinations(link_list)
            criteria = ["combination1", "combination2", "combination3", "combination4", "combination5", "combination6"]
        else:
            criteria = ["Nombre"]
        return link_list, criteria


    @classmethod
    def prepare_result_set(cls, result_set, link_list, algorithm_name, criterion, scores):
        result_set['Nombre_EL'] = result_set['Nombre1'].astype(str)
        result_set["idSolicitud"] = link_list["idSolicitud"]
        result_set["idNumber"] = link_list["idNumber"]
        result_set["Nombre"] = link_list["Nombre"]
        result_set["Nombre1"] = link_list["Nombre1"]
        result_set["Nombre2"] = link_list["Nombre2"]
        result_set["Apellido1"] = link_list["Apellido1"]
        result_set["Apellido2"] = link_list["Apellido2"]
        result_set["algorithm"] = algorithm_name
        result_set["criterio"] = criterion
        result_set["Resultado"] = scores
        return result_set


    @classmethod
    def prepare_result_set_string(cls, result_set, string, algorithm_name, scores):
        result_set['Nombre1'] = result_set['Nombre1'].astype(str)
        result_set["Nombre"] = string
        result_set["algorithm"] = algorithm_name
        result_set["Resultado"] = scores
        return result_set


    @classmethod
    def iter_sample(cls,path_sample, path_rv, path_el, cols, alg, name, threshold):
        print("Reading data")
        data_sample, entradas_listas = read_data(path_sample, path_rv, path_el, cols)
        print("Starting algorithm calculation")
        results = data_sample.mapply(cls.find_matches,axis=1,args=(entradas_listas, alg, name, threshold,cols, path_rv))
        result, duration = map(list,zip(*results.to_list()))
        data_sample["duration"] = duration
        final_results = pd.concat(result).reset_index(drop=True)
        print("Exporting Data")
        storage.save_df_to_storage(final_results,name)
        storage.save_df_to_storage(data_sample, name, is_time=True)
        

    @classmethod
    def find_matches(cls, link_list, inputs, algorithm, algorithms_name, threshold, cols, path_rv):        
        init = time.time()
        link_list, criteria = cls.prepare_link_list(link_list)
        results_sets = []
        for criterion in criteria:
            results = inputs.apply(lambda x: algorithm(x['Nombre1'], link_list[criterion]), axis=1) * 100
            filt = results >= threshold
            if any(filt):
                result_set = inputs[filt].copy()
                scores = results[filt]
                result_set = cls.prepare_result_set(result_set, link_list, algorithms_name, criterion, scores)
                results_sets.append(result_set)
        if len(results_sets) > 0:
            total_results = pd.concat(results_sets)
        else:
            total_results = pd.DataFrame(columns=cols)
        end = time.time() - init
        return (total_results, end)

    @classmethod
    def find_matches_for_algorithms(cls, inputs, link_list, algorithm, algorithm_name, threshold):
        link_list, criteria = cls.prepare_link_list(link_list)
        results_sets = []
        for criterion in criteria:
            results = inputs.apply(lambda x: algorithm(x['Nombre1'], link_list[criterion]), axis=1) * 100
            filt = results >= threshold
            if any(filt):
                result_set = inputs[filt].copy()
                scores = results[filt]
                result_set = cls.prepare_result_set(result_set, link_list, algorithm_name, criterion, scores)
                results_sets.append(result_set)
        total_results = pd.concat(results_sets)
        return total_results

    @classmethod
    def find_matches_string(cls, inputs, string, algorithms, algorithms_name, threshold):
        results_sets = []
        for idx, algorithm in enumerate(algorithms):
                results = inputs.apply(lambda x: algorithm(x['Nombre1'], string), axis=1) * 100
                filt = results >= threshold
                if any(filt):
                    result_set = inputs[filt].copy()
                    scores = results[filt]
                    result_set = cls.prepare_result_set_string(result_set, string, algorithms_name[idx],scores)
                    results_sets.append(result_set)
        total_results = pd.concat(results_sets)
        return total_results