# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 09:49:16 2022

@author: mathe
"""

import re
from tqdm import tqdm
import json
import pandas as pd

def network_clusters(clusters, max_thres = 500, min_thres = 0):
    edge_list= []

    for cluster in tqdm(clusters):
        
        books_in = clusters[cluster]
        bcount = len(books_in)
        if bcount < max_thres and bcount > min_thres:
            books_copy = books_in[:]
            for book1 in books_in:        
                books_copy.remove(book1)
                if len(books_copy) > 0:
                    for book2 in books_copy:
                        bid1 = book1.split("-")[0]
                        bid2 = book2.split("-")[0]
                        edge_list.append([bid1, bid2])
    df_edges = pd.DataFrame(edge_list, columns = ["book1", "book2"])
    df_edges["count"] = 1
    df_edges = df_edges.groupby(["book1", "book2"])["count"].count().reset_index()
    
    return df_edges                       

meta_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5_merged_wNoor.csv"
cluster_path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/clusters.json"
    

def create_nodes(edges_df, metadata_df, on_authors= False):
    # Get node list
    node_df = pd.DataFrame()
    node_df["id"] = edges_df["Source"]
    temp_df = pd.DataFrame()
    temp_df["id"] = edges_df["Target"]
    node_df = pd.concat([node_df, temp_df])
    node_df = node_df.drop_duplicates()
    
    # Merge in metadata
    if not on_authors:
        metadata_df = metadata_df[["id", "date", "book"]]
        node_df = pd.merge(node_df, metadata_df, on ="id")
        
    else:
        metadata_df = metadata_df[["date", "book"]]
        metadata_df[["id", "book"]] = metadata_df["book"].str.split(".", expand=True)
        metadata_df = metadata_df.drop(columns = ["book"])
        metadata_df = metadata_df.drop_duplicates()
        node_df = pd.merge(node_df, metadata_df, on ="id")
    
    return node_df

def filter_edges(edges_df, metadata_df, merge_author = True, min_weight = 0, max_date = 1500):
    date_df = metadata_df[["id", "date"]]
    date_df = date_df.rename(columns = {"id": "Source"})
    edges_df = pd.merge(edges_df, date_df, on ="Source")
    date_df = date_df.rename(columns={"Source": "Target", "date" : "date_t"})
    edges_df = pd.merge(edges_df, date_df, on ="Target")
    edges_df = edges_df[(edges_df["date"] <= max_date) & (edges_df["date_t"] <= max_date)]
    
    
    if merge_author:    
        metadata_df = metadata_df[["id", "book"]]
        metadata_df[["s_author", "book"]] = metadata_df["book"].str.split(".", expand=True)
        metadata_df = metadata_df.rename(columns = {"id": "Source"})
        edges_df = pd.merge(edges_df, metadata_df, on ="Source")
    
        metadata_df = metadata_df.rename(columns={"Source": "Target", "s_author": "t_author"})
        edges_df = pd.merge(edges_df, metadata_df, on ="Target")
        
        new_edges_df = edges_df[["s_author", "t_author", "weight"]]
        new_edges_df = new_edges_df.rename(columns={"s_author": "Source", "t_author" : "Target"})
        new_edges_df = new_edges_df.groupby(["Target", "Source"]).agg({'weight' : 'sum'}).reset_index()
    return new_edges_df

with open(cluster_path) as f:
    clusters = json.load(f)
    f.close()

meta_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5_merged_wNoor.csv"
metadata_df = pd.read_csv(meta_path, sep = "\t")
    
edges = network_clusters(clusters, meta_path, min_thres = 2)
                
author_edges = filter_edges(edges, metadata_df, max_date = 900)
author_nodes = create_nodes(author_edges, metadata_df, on_authors=True)          
            
            