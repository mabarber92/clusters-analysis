# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 09:37:08 2022

@author: mathe
"""

import pandas as pd

df_path = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/edges.csv"
edges_df = pd.read_csv(df_path)
edges_df = edges_df.rename(columns = {"book1": "Source", "book2" : "Target", "count" : "weight"})
meta_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5_merged_wNoor.csv"
metadata_df = pd.read_csv(meta_path, sep = "\t")

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

author_edges = filter_edges(edges_df, metadata_df, max_date = 900)
author_nodes = create_nodes(author_edges, metadata_df, on_authors=True)   
# node_df = create_nodes(edges_df, metadata_df)
    
    