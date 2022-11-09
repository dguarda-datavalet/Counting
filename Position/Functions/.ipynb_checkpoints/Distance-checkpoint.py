import itertools
from itertools import chain
import numpy as np
import math
import pandas as pd
pd.options.mode.chained_assignment = None
import copy
import uuid
from numpy import linalg as LA

channel_df = pd.read_csv(f'channel.csv')

def distance_function(fspl, frequency):
    '''
    Returns a distance in meter from the parameters:
    fspl: basically the signal strength observed in absolute value
    frequency: the frequency in ghz
    '''
    return 10**((abs(fspl)-(20*math.log(frequency, 10))-32.45)/20)

def return_channel_frequency(channel_df, channel_num):
    return channel_df[channel_df['CHANNEL NUMBER']==channel_num]['FREQUENCY']/1000

def cycle_and_apply(x):
    signal_strength_list = []
    ap_name_list = []
    for ap_observation in x:
        signal_strength_list.append(distance_function(ap_observation[1], ap_observation[2]))
        ap_name_list.append(ap_observation[0])
        
    return signal_strength_list, ap_name_list, len(x)

def generate_uuid(x):
    return uuid.uuid4()

def transform_dict_entry(x, column, new_colum, fct):
    x[new_colum] = x[column].apply(lambda x: fct(x))
    return x

def explode_outer(x, column, new_columns):
    x[new_columns] = pd.DataFrame(x[column].tolist())
    return x

def fetch_one_item(master_dict, key, item_position):
    return master_dict[key].loc[item_position]

def compare_set_column(x, list_to_compare):
    return set(x) == set(list_to_compare)

def subs_list(x, float_to_subs):
    return [LA.norm(np.subtract(np.array(x),np.array(float_to_subs)))]

def compare_list_distance(x, boundary_to_compare):
    return x[0] < boundary_to_compare

def compute_distance_window(master_dict, item, key, discriminant_walking, time_window_length):
    working_df = master_dict[key][master_dict[key]['number_ap'] == item['number_ap']]
    working_df['ap_name_bool'] = working_df['ap_name'].apply(lambda x: compare_set_column(x,item['ap_name']))
    working_df = working_df[working_df['ap_name_bool']]
    
    boundary = discriminant_walking*time_window_length
    if(len(working_df)>0):
        working_df['distance'] = working_df['distance'].apply(lambda x: subs_list(x,item['distance']))
        working_df['distance_bool'] = working_df['distance'].apply(lambda x: compare_list_distance(x,boundary))
        working_df = working_df[working_df['distance_bool']]
        #adding the item name as a column
        working_df['source_device_mac'] = item['device_mac']
    else:
        return None
    try:
        return working_df[['source_device_mac','device_mac','distance','ap_name','number_ap']].to_dict('records')
    except:
        return None

def compute_n_windows(keys, **kwargs):
    starting_disc = kwargs['discriminant_walking']
    item = kwargs['item']['device_mac']
    dict_of_dicts = {}
    i = 1.0
    for key in keys:
        kwargs['discriminant_walking'] = starting_disc * i
        computed_distance = compute_distance_window(key = key, **kwargs)
        if(computed_distance is not None):
            dict_of_dicts[f'{key}_{item}'] = computed_distance
            i = i+1
    return dict_of_dicts