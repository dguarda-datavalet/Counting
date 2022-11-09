from itertools import chain
import pandas as pd
from itertools import repeat

def recreate(callable, count=None):
    for c in repeat(callable, count):
        for val in c():
            yield val
            
def dicts_to_dataframe(dicts):
    return pd.DataFrame.from_dict(dicts)

def unchain(dicts):
    return chain.from_iterable(dicts)