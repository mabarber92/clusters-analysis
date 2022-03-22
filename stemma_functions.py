# -*- coding: utf-8 -*-
"""
Created on Sun Jan 23 12:37:34 2022

@author: mathe
"""
import json
from tqdm import tqdm

def node_concat_main(ms_id, ms_pos, cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap):
    if level >= max_l:
        
        return nodes, edges
    if len(cluster_list) == 0:

        return nodes, edges

    else:
        
        
        books_list = []
        new_cluster_list = []

        for cl in cluster_list:
            if str(cl) in clusters_dict.keys():
                ms_listed = clusters_dict[str(cl)]
                del clusters_dict[str(cl)]
                if len(ms_listed) > cluster_cap:
                    continue
                else:
                    books_list.extend(ms_listed)
        books_list = list(dict.fromkeys(books_list))


        for book in books_list:
            au_title = book.split(".")[0]
            ms_dict = books_dict[au_title]

            for new_cl in ms_dict[book]:
                
                if new_cl in cluster_list:
                    continue
                else:                 
                    new_cluster_list.append(new_cl)
        new_cluster_list = list(dict.fromkeys(new_cluster_list))
        

        
        # for old_cl in cluster_list:
        #     if old_cl in new_cluster_list:
        #         new_cluster_list.remove(old_cl)
        
        pr_lid = str(ms_pos) + "-l" + str(level)
        level = level + 1        
        lid = str(ms_pos) + "-l" + str(level)
        nodes.append({'data': {'id': lid, 'label': str(len(cluster_list)) + " clusters, " + str(len(books_list)) + " books"},
                      'position': {'x': ms_pos, 'y': level}})
        edges.append({'data': {'source': pr_lid, 'target': lid}})
        return node_concat_main(ms_id, ms_pos, new_cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap)

# Function to draw the main stemma
def build_main_stemma(book, cluster_dict, book_dict, max_level = 10, cluster_cap = 500):
    ms_list = book_dict[book].keys() 
    nodes = []
    edges = []
    for ms in tqdm(ms_list):
       cluster_copy = cluster_dict.copy()
       ms_pos = int(ms.split(".ms")[-1])
       level = 1
       nodes.append({'data':{ 'id': str(ms_pos) + "-l" + str(level) , 'label': ms_pos},
                     'position' : {'x' : ms_pos, 'y' : 1}})
       edge_clusters = book_dict[book][ms]
       
       nodes, edges = node_concat_main(ms, ms_pos, edge_clusters, book_dict, cluster_copy, nodes, edges, level, max_level, cluster_cap)
    return nodes, edges

def stemma_expander(ms_id, prev_ms_pos, prev_len, cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap, x_spacing, y_spacing, all_books = []):
    if level >= max_l or len(cluster_list) == 0:
        
        ### Computer nodes from booklist
        max_width = len(all_books[-1]) * x_spacing
        y_level = 2
        for book_ms_list in all_books:
            y_pos = y_level * y_spacing
            y_level = y_level + 1
            level_width = len(book_ms_list) * x_spacing
            star_pos = prev_ms_pos + (prev_len/2) + ((max_width - level_width)/2)
            for idx, book_ms in enumerate(book_ms_list):
                x_pos = star_pos + (idx*x_spacing)
                nodes.append({'data': {'id': book_ms, 'label': book_ms},
                      'position': {'x': x_pos, 'y': y_pos}})
                
        
        ms_pos = prev_ms_pos + (prev_len/2) + (max_width/2) 
        
        return nodes, edges, ms_pos
   

    else:
        
        
        books_list = []
        new_cluster_list = []

        for cl in cluster_list:
            if str(cl) in clusters_dict.keys():
                ms_listed = clusters_dict[str(cl)]
                del clusters_dict[str(cl)]
                if len(ms_listed) > cluster_cap:
                    continue
                else:
                    books_list.extend(ms_listed)
        books_list = list(dict.fromkeys(books_list))
        all_books.append(books_list)

        for book in books_list:
            books_list_copy = books_list[:]
            books_list_copy.remove(book)
            au_title = book.split(".")[0]
            ms_dict = books_dict[au_title]
            for book2 in books_list_copy:
                edges.append({'data': {'source': book, 'target': book2}})

            for new_cl in ms_dict[book]:
                
                if new_cl in cluster_list:
                    continue
                else:                 
                    new_cluster_list.append(new_cl)
        new_cluster_list = list(dict.fromkeys(new_cluster_list))
        level = level + 1  
        
        
        return stemma_expander(ms_id, prev_ms_pos, prev_len, new_cluster_list, books_dict, clusters_dict, nodes, edges, level, max_l, cluster_cap, x_spacing, y_spacing)    

def build_stemma(ms_list, cluster_dict, book_dict, max_level = 3, cluster_cap = 500, x_spacing = 100, y_spacing = 50):
    nodes = []
    edges = []
    prev_ms_pos = 0
    for ms in ms_list:
       book = ms.split(".")[0]
       cluster_copy = cluster_dict.copy()
       edge_clusters = book_dict[book][ms]
       nodes, edges, prev_ms_pos = stemma_expander(ms, prev_ms_pos, 0, edge_clusters, book_dict, cluster_copy, nodes, edges, 1, max_level, cluster_cap, x_spacing, y_spacing)
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

nodes, edges = build_stemma(book_ms, clusters_data, books, max_level = 3)