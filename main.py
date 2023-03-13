from fastapi import FastAPI
from domain.usecases.process_matches.determine_matches import DetermineMatches

from domain.models.request.algorithm import Algorithm
from domain.models.request.item import Item

app = FastAPI()


@app.post('/matches')
async def determine_matches(item: Item):
    return DetermineMatches.iter_string(item)


@app.get('/')
async def show_algorithms_table():
    return {"Jaro Winkler": 1,
            "Normalized Levenshetein": 2,
            "Cosine": 3,
            "Jaccard": 4,
            "Sorensen Dice": 5,
            "Fuzzy Wuzzy": 6
            }


@app.post('/match')
async def determine_matches_for_algorithm(algorithm: Algorithm):
    DetermineMatches.exec(algorithm.algorithm_index)
