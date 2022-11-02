import copy
from matplotlib import pyplot as plt
import pandas as pd
import statistics as sts
import numpy as np
from statsmodels import robust
from itertools import islice

def window(seq, n=2):
    '''
    Returns a sliding window (of width n) over data from the iterable
       s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   
    '''
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def mid_point(x1, x2):
    return int((x1 + x2) / 2)
        
def correct_overlapping_index(tuple_array):
    '''
    Creates non overlapping values from tuple arrays and corrects the first tuple
    '''
    return [tuple_array[0]] + [(x[0]+1,x[1]) for x in tuple_array[1:]]   
        
def get_series_index(series, value, max_bool = True):
    '''
    Map value to corresponding index
    '''
    return (series[series >= value[0]].index[0], series[series <= value[1]].index[-1])

def split_dict(series, master_dict):
    resulting_dict = {}
    for key, value in master_dict.items():
        splitted_dict = split_window(series, key, value)
        #merging dictionnaries
        resulting_dict = {**resulting_dict, **splitted_dict}
    return resulting_dict
 
def split_window(series, tuple_value, tuple_index):
    '''
    Returns the corresponding mid point of value and maps it to the series object
    '''
    mid_value_list = [tuple_value[0], mid_point(tuple_value[0], tuple_value[1]), tuple_value[1]]
    level_value_array = correct_overlapping_index(list(window(mid_value_list)))
    # Initiating dict
    value_index_dict = {}
    # Map the two new values to the dataset itself and return indexes
    for value_tuple in level_value_array:
        value_index_dict[value_tuple] = get_series_index(series, value_tuple)
        
    return value_index_dict

def calculate_variance(series, master_dict, sorting_column='Timestamp'):
    '''
    Computing variance of the resulting time series based on the slices computed from the split_window_function
    '''
    variance_list = []
    for key, value in master_dict.items():
        variance_list.append(abs(value[1]-value[0])) 
        
    return robust.mad(variance_list), min(variance_list)  

def fine_split_algorithm(starting_values, master_dict, min_value_len = 4, min_window_len = 3, min_iterations = 6, tolerance = 0):
    '''
    Splits a dataset in half until either:
     - the mean absolute deviation increases and we reached at least a minimum amount of iteration
     - We reach a minimum length of the window size
    '''
    #Initiating booleans for algorithm
    stop_counter = 0
    last_variance = np.inf
    actual_iterations = 0
    value_length = np.inf
    window_length = np.inf

    while(True):
        master_dict = split_dict(starting_values, master_dict)
        current_variance = calculate_variance(starting_values, master_dict)
        if(current_variance[0] > last_variance):
            stop_counter = stop_counter + 1
        last_variance = current_variance[0]
        value_length = current_variance[1]   
        window_length = next(iter(master_dict))[1]-next(iter(master_dict))[0]
        
        if(stop_counter>tolerance and min_iterations<actual_iterations):
            break    # break here
        if(value_length<=min_value_len):
            break    # break here
        if(window_length<=min_window_len):
            break    # break here
        actual_iterations = actual_iterations + 1
    return master_dict

def generate_window_dict(dataset, sorting_column, window_size=1800, window_frequency=900, **kwargs):
    '''
    Takes a dataset and outputs a index based on args: min_window_len, min_iterations, tolerance
    min_window_len: length of the window generated
    min_iterations: wont stop until n iterations
    tolerance: number of time until the algorithm stops when the variations keeps coming up
    '''
    starting_values = dataset.sort_values(sorting_column).reset_index()[sorting_column]
    #initiating dictionnary
    min_value, max_value = min(starting_values), max(starting_values)
    master_dict = {}
    #creating level 0 index and removing the overlap between the values
    level_0 = range(min_value, max_value+window_frequency, window_frequency)
    level0_index_array = correct_overlapping_index(list(window(level_0)))
    
    #mapping the value to the index
    for tuple_array in level0_index_array:
        master_dict[tuple_array] = get_series_index(starting_values, tuple_array)
    
    #running algorithm 
    master_dict = fine_split_algorithm(starting_values, master_dict, **kwargs)

    return master_dict