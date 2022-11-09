import pandas as pd
import numpy as np
import networkx as nx
from itertools import chain
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import pydot
import pygraphviz

import sys  
sys.path.insert(0, './Functions/')

import Window

def spew_tuples(obj, seq_pourc = 0.05):
    '''
    Takes the window index and returns the range split by a % for instance, 10% would return a dataset split in 10 parts...s
    '''
    total_slices = len(obj.window_index)
    slice_width = int(total_slices*seq_pourc)
    starting_range = range(0,total_slices,slice_width)
    if (list(starting_range)[-1])==(total_slices-1):
        return Window.window(starting_range, 2)
    else:
        return Window.window(chain(starting_range, [total_slices]), 2)
    
    return starting_range

def correct_overlapping_index(tuple_array, lag = -5):
    '''
    Creates non overlapping values from tuple arrays and corrects the first tuple
    '''
    return [tuple_array[0]] + [(x[0]+lag,x[1]) for x in tuple_array[1:]]   

def draw_graph_dot(obj, prog = 'dot'):
    '''
    Draws the dot directed graph using pygraphviz and network x
    '''
    pos = nx.nx_pydot.graphviz_layout(obj.graph, prog=prog)
    figure(figsize=(24, 18), dpi=80)
    nx.draw_networkx_nodes(obj.graph, pos, node_size = 8)
    nx.draw_networkx_edges(obj.graph, pos, arrows=True, width=0.2)
    plt.show()

def flatten_once(d):
    out = {}
    for key, val in d.items():
        out.update(val)
    return out

def put_in_list(d):
    out = []
    for key, val in d.items():
        out.append(val)
    return out