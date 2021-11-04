# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 10:22:41 2021

@author: mathe
"""

import pandas as pd
import os
import json
import pickle
from tqdm import tqdm

def cluster_books(json_path):
    books_out = {}
    clusters_out = {}
    script_path = os.path.dirname(__file__)
    temp_file_cl = script_path + "/temp_pickle_cl"
    temp_file_bk = script_path + "/temp_pickle_bk" 
    print(temp_file_cl)
    print(temp_file_bk)
    os.chdir(json_path)
    
    
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            json_path = os.path.join(root, name)
            print(json_path)
            print("Getting unique vals")
            # Get list of unique vals for file
            data = pd.read_json(json_path, lines = True)[["cluster", "id", "series"]]
            unique_b = list(dict.fromkeys(data["id"]))
            print("No. of unique books: " + str(len(unique_b)))
            unique_c = list(dict.fromkeys(data["cluster"]))
            print("No. of unique clusters: " + str(len(unique_c)))
            
            print("Getting clusters by books")
            # Filter by books and get cluster lists for each book ms and categorise by book
            for book in tqdm(unique_b):
                clusters = data.loc[data["id"] == book]["cluster"].values.tolist()
                bid = book.split(".")[0]
                if bid in books_out:                    
                    books_out[bid].append({book : clusters})
                else:
                    books_out[bid] = [{book : clusters}]
            
            outfile = open(temp_file_bk, 'wb')
            pickle.dump(books_out, outfile)
            outfile.close()
            
            print("Getting books by clusters")
            # Filter by clusters and write books for each cluster to dict
            for cluster in tqdm(unique_c):
                books = data.loc[data["cluster"] == cluster]["id"].values.tolist()
                if cluster in clusters_out:
                    
                    clusters_out[cluster].extend(books)
                    
                else:
                    clusters_out[cluster] = books
                    
                
            outfile = open(temp_file_cl, 'wb')
            pickle.dump(clusters_out, outfile)
            outfile.close()
    
    return books_out, clusters_out


json_path = "D:/Corpus Stats/2021/Cluster data/full_clusters/out.json"
books, clusters = cluster_books(json_path)   