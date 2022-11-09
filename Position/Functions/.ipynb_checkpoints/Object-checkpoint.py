import networkx as nx
import math
import pandas as pd
import numpy as np
import itertools
from itertools import chain

import sys  
sys.path.insert(0, './Functions/')

import Window
import Distance

def distance_function(fspl, frequency):
    '''
    Returns a distance in meter from the parameters:
    fspl: basically the signal strength observed in absolute value
    frequency: the frequency in ghz
    '''
    return 10**((fspl-(20*math.log(frequency, 10))-32.45)/20)

class Position_graph:
    def __new__(cls, *args, **kwargs):
        print(f"New empty graph")
        return super().__new__(cls)

    def __init__(self, dataset, sorting_columns = ['Timestamp','device_mac','ap_mac']):
        self.dataset = dataset.sort_values(sorting_columns).reset_index(drop=True)
        print(f"Length of dataset: {dataset.shape[0]}")
        self.walking_param = 0.85
        self.distance_formula = distance_function
        self.channel_table = pd.read_csv(f'channel.csv')
        #Initiating complex structures
        
        self.window_kwargs = {
            'min_window_len':3,
            'min_value_len':1,
        }
        
        self.distances = ''
        self.active_distance_slice = ''
        self.window_index = ''
        self.graph_dict = {}
        self.graph = 'Please initiate the graph using the function generate_graph_structure()'
        self.subgraph = 'Please initiate the graph using the function generate_subgraphs()'
        self.topological_order = 'Please initiate the graph using the function generate_topological_order()'
        
        
    def compute_window(self, window_size=1800, window_frequency=900):
        '''
        Calling the window function passing the kwargs from the object
        '''
        dummydataset = pd.DataFrame.copy(self.dataset, deep= True)
        dummydataset['signal_strength'] = dummydataset[['ap_mac','signal_strength', 'channel']].values.tolist()
        self.window_index = Window.generate_window_dict(dummydataset, 'Timestamp', window_size=1800, window_frequency=900, **self.window_kwargs)
    
    def compute_distance_in_windows(self, start_slice=0, stop_slice=100):
        '''
        From the window structure we compute a slice of distances (based on n windows) returns the active_distance_slice structure having the   
        distances computed for each random observations
        '''
        dummydataset = pd.DataFrame.copy(self.dataset, deep= True)
        dummydataset['signal_strength'] = dummydataset[['ap_mac','signal_strength', 'channel']].values.tolist()
        
        if self.window_index == '':
            self.window_index = Window.generate_window_dict(dummydataset, 'Timestamp', window_size=1800, window_frequency=900, **self.window_kwargs)
    
        sub_window_index = dict(itertools.islice(self.window_index.items(), start_slice, stop_slice))
        df_list = {}

        for key, value in sub_window_index.items():
            df_list[key] = dummydataset[value[0]:value[1]].groupby(['device_mac'])['signal_strength'].apply(lambda x: list(np.unique(x))).apply(list).reset_index()
        
        #computing the distance by observation and by ap
        final_dict = dict((k, Distance.transform_dict_entry(v, 'signal_strength', 'distance_by_ap', Distance.cycle_and_apply)) for k,v in df_list.items())  
        #exploding the array produced into a distance column and coresponding ap_name column and the numbers of ap encountered
        final_dict = dict((k, Distance.explode_outer(v, 'distance_by_ap', ['distance','ap_name','number_ap'])) for k,v in final_dict.items())
        #cleaning the resulting array
        final_dict = dict((k, v.drop(['signal_strength', 'distance_by_ap'], axis=1)) for k,v in final_dict.items())
        #replacing the device_mac by a true random uuid in order to avoid breaking the next algorithms
        final_dict = dict((k, Distance.transform_dict_entry(v, 'device_mac', 'device_mac', Distance.generate_uuid)) for k,v in final_dict.items()) 
        self.active_distance_slice = final_dict
        print(f'Slice from {start_slice} to {stop_slice} computed for distance stored under the active_distance_slice attribute')
    
    def compute_distance_1_to_n(self, n_window=5, n_starting_index = 0, starting_slice = 0):
        '''
        By index, in a starting slice connect to the next n windows and return the dictionnaries of tuple of source to destination and edge values
        '''
        if self.active_distance_slice == '':
            raise Exception('Please compute compute_distance_in_windows before this function!')
        #storing the keys only
        dict_keys = list(self.active_distance_slice.keys())
        first_item = Distance.fetch_one_item(self.active_distance_slice, dict_keys[starting_slice], n_starting_index)
        kwargs = {
            'discriminant_walking' : self.walking_param,
            'time_window_length' : dict_keys[0][1]-dict_keys[0][0],
            'master_dict' : self.active_distance_slice,
            'item' : first_item
        }
        dicts =  Distance.compute_n_windows(dict_keys[starting_slice+1:starting_slice+1+n_window], **kwargs)
        return dicts
    
    def compute_full_window_distance(self, n_window = 5, starting_slice = 0):
        '''
        Compute the a complete slice and return the dictionnaries of tuples
        '''
        dict_list_graph_node = {}
        dict_keys = list(self.active_distance_slice.keys())
        number_of_nodes = len(self.active_distance_slice[dict_keys[starting_slice]])
        for i in range(0,number_of_nodes):
            obs_graph_struct = self.compute_distance_1_to_n(n_starting_index = i, n_window = n_window, starting_slice = starting_slice)
            if len(obs_graph_struct)>0:
                dict_list_graph_node[f'{dict_keys[starting_slice]}_{i}'] = obs_graph_struct
        return dict_list_graph_node
    
    def generate_graph_structure(self):
        return None
    
    def generate_topological_order(self):
        return None
    
    def generate_subgraphs(self):
        return None
        
    def __repr__(self) -> str:
        return f"{type(self).__name__} is a graph structure with the following attributes:"