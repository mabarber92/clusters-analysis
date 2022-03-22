# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 14:57:39 2022

@author: mathe
"""

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
import dash_cytoscape as cyto
import dash_daq as daq
import json
import os
import pandas as pd
import re
from matplotlib import cm
from matplotlib.colors import rgb2hex


app = dash.Dash(prevent_initial_callbacks=True)




def stemma_expander(ms_id, prev_ms_pos, prev_len, cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap, x_spacing, y_spacing, all_books = []):
    if level >= max_l or len(cluster_list) == 0:
        
        ### Computer nodes from booklist
        
        max_width = len(all_books[-1]) * x_spacing
        y_level = 2
        print(all_books)
        for book_ms_dict in all_books:
            y_pos = y_level * y_spacing
            lid = "-l-" + str(y_level)
            prelid = "-l-" + str(y_level - 1)
            y_level = y_level + 1
            level_width = len(book_ms_dict) * x_spacing
            star_pos = prev_ms_pos + (prev_len/2) + ((max_width - level_width)/2)
            for idx, book_ms_item in enumerate(book_ms_dict):
                star_pos = star_pos + (idx*x_spacing)
                for idx2, book2 in enumerate(book_ms_item["book2s"]):
                    x_pos = star_pos + (idx2*x_spacing)
                    b1id = book_ms_item["book1"] + prelid
                    b2id = book2 + lid                    
                    nodes.append({'data': {'id': b2id, 'label': b2id},
                      'position': {'x': x_pos, 'y': y_pos}})
                    edges.append({'data': {'Source': b1id, 'Target' : b2id}})
                
        
        ms_pos = prev_ms_pos + (prev_len/2) + (max_width/2) 
        
        return nodes, edges, ms_pos
   

    else:
        
        
        books_list = []
        new_cluster_list = []

        for cl in cluster_list:
            if str(cl[1]) in clusters_dict.keys():
                ms_listed = clusters_dict[str(cl[1])]
            
                del clusters_dict[str(cl[1])]
                if len(ms_listed) > cluster_cap:
                    continue
                else:                   
                    ms_listed.remove(cl[0])
                    books_list.append({"book1": cl[0], "book2s": ms_listed})
        # books_list = list(dict.fromkeys(books_list))
        all_books.append(books_list)
        
        for book in books_list:
            for book2 in book["book2s"]:
            
            
                au_title = book2.split(".")[0]
                ms_dict = books_dict[au_title]
                # 
        
                for new_cl in ms_dict[book2]:
                    new_cluster_list.append([book2, new_cl])
        
        level = level + 1  
        
        
        return stemma_expander(ms_id, prev_ms_pos, prev_len, new_cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap, x_spacing, y_spacing)    

def build_stemma(ms_list, cluster_dict, book_dict, max_level = 3, cluster_cap = 500, x_spacing = 100, y_spacing = 150):
    nodes = []
    edges = []
    prev_ms_pos = 0
    for ms in ms_list:
       book = ms.split(".")[0]
       cluster_copy = cluster_dict.copy()
       edge_clusters = book_dict[book][ms]
       first_cl_list = []
       for e_cl in edge_clusters:
           first_cl_list.append([ms, e_cl])
       nodes, edges, prev_ms_pos = stemma_expander(ms, prev_ms_pos, 0, first_cl_list, book_dict, cluster_copy, nodes, edges, 1, max_level, cluster_cap, x_spacing, y_spacing)
       nodes.append({'data':{ 'id': ms, 'label': ms},
                     'position' : {'x' : prev_ms_pos, 'y' : 1 * y_spacing}})
       
       
       
       
    return nodes + edges

book_path = "C:/Users/mathe/Documents/Kitab project/Visualisations/cluster_ms_book/data/Oct_2021/books_dict.json"
cluster_path = "C:/Users/mathe/Documents/Kitab project/Visualisations/cluster_ms_book/data/Oct_2021/clusters.json"
book = "Shamela0000176-ara1"

book_ms = ["Shamela0012724-ara1.mARkdown.ms073"]

with open(book_path) as f:
    books = json.load(f)
    f.close()

with open(cluster_path) as f:
    clusters_data = json.load(f)
    f.close()

net = build_stemma(book_ms, clusters_data, books, max_level = 3)


app.layout = html.Div(["test text", dcc.Loading(cyto.Cytoscape(id='main-stemma',
                                            layout={'name': 'preset'},
                                            style={'width': '100%', 'height': '600px'},
                                            elements= net
                                            ))])

if __name__ == '__main__':
    app.run_server(debug=True)