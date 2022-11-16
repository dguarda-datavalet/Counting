import networkx as nx
import math
import pandas as pd
import numpy as np
from uuid import UUID
import operator

from itertools import chain
from itertools import product
from itertools import starmap
from functools import partial

import sys  
sys.path.insert(0, './Functions/')

import Graph

def search_uuid_in_slices(active_distance_slice, node):
    '''
    Retreiving the uuid field in the active distance tables to retrieve the unix timestamp
    '''
    distance_slice_keys = list(active_distance_slice.keys())
    for key in distance_slice_keys:
        if len(active_distance_slice[key][active_distance_slice[key]['device_mac']==node])>0:
            return key
    return None

def set_nodes_time_window_attribute(active_distance_slice, graph, nodes_list):
    '''
    Taking the object and adding the time attribute to individual nodes
    '''
    nx.set_node_attributes(graph, None, "time_window")
    for node in nodes_list:
        node_time_window = search_uuid_in_slices(active_distance_slice, node)
        graph.nodes[node]['time_window'] = node_time_window[0]
    return None


def generate_topological_order(obj):
    obj.topological_order = nx.topological_sort(nx.line_graph(obj.graph))
    
def test_and_keep_leaf(path_dict, leaves):
    '''
    Test if a path actually ends with a leaf, only keep these ones, delete the rest
    '''
    single_source_keys = list(path_dict.keys())
    for key in single_source_keys:
        if not (path_dict[key][-1] in leaves):
            del path_dict[key]
    return path_dict

def generate_path_from_roots_to_leaves(graph, roots, leaves):
    '''
    Generate all path from the graph that actually span from root to leaf.
    '''
    final_dict = {}
    total_paths = 0
    for root in roots:
        #get all the paths starting from the roots
        path_dict = dict(nx.single_source_bellman_ford_path(graph, source=root, weight='distance'))
        #keep the paths that 
        path_computed = test_and_keep_leaf(path_dict, leaves)
        final_dict[root] = path_computed
        total_paths = total_paths + len(list(path_computed.keys()))
    print(f"Computed {len(list(final_dict.keys()))} roots with a total of {total_paths} total paths!")
    return final_dict

def compute_distance_for_paths(graph, all_paths_list):
    distance_path_list = []
    for path in all_paths_list:
        pathGraph = nx.path_graph(path)
        distance_path_list.append((path, average_distance_per_path(graph, pathGraph)))
    return distance_path_list
        
def average_distance_per_path(graph, path):
    sum_distance = 0
    length_path = len(path)
    for ea in path.edges():
        #print from_node, to_node, edge's attributes
        sum_distance = sum_distance + graph.edges[ea[0], ea[1]]['distance']
    return (sum_distance, length_path)

def unpack_items(items):
    return [*items]

def remove_nodes_from_graph(graph, nodes):
    graph.remove_nodes_from(nodes)

def is_path(graph, path):
    try:
        return nx.is_simple_path(graph, path) 
    except:
        return False
    
def remove_devices_from_graph(graph, pourc=0.5):
    '''
    Compute all the paths from root to leaves and remove the lowest n from the graph based on % of the paths available.
    This function modifies the graph and returns a list of resulting sub graphs
    '''
    devices = []
    roots = [v for v, d in graph.in_degree() if d == 0]
    leaves = [v for v, d in graph.out_degree() if d == 0]

    all_paths_root_to_leaf = generate_path_from_roots_to_leaves(graph, roots, leaves)
    #unpacking the structure generated from the previous function call
    all_paths_list_root_to_leaf = Graph.put_in_list(all_paths_root_to_leaf)
    list_root_to_leaf_unchained = list(chain.from_iterable([unpack_items(d.values()) for d in all_paths_list_root_to_leaf]))
    #calculating the distance in each path to minimize it later
    root_to_leaf_paths_distance = compute_distance_for_paths(graph, list_root_to_leaf_unchained)
    #sorting by distance accrued
    root_to_leaf_paths_distance.sort(key=lambda x: x[1][0])
    
    
    for path in root_to_leaf_paths_distance[0:int(len(root_to_leaf_paths_distance)*pourc)]:
        if is_path(graph, path[0]):
            if(len(path[0])>2):
                devices.append([graph.subgraph(path[0]).copy(),path[1]])
            remove_nodes_from_graph(graph, path[0])
    print(f'    Found {len(devices)} device!')        
    return devices