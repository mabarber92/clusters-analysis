# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 09:27:09 2022

@author: mathe
"""

import parquet
import json
import pyarrow.parquet as pq
import os
import pandas as pd
from tqdm import tqdm

def parquet_books_clusters(parquet_path, books_out_path, clusters_out_path):
    # Empty dictionaries to write output
    books_out = {}
    clusters_out = {}
    os.chdir(parquet_path)
    # For eact parquet file unpack the data and categorise into the two dictionaries - one for books and their clusters, one for clusters and their books
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            pq_path = os.path.join(root, name)
            print(pq_path)
            data = pq.read_table(pq_path).to_pandas()[["cluster", "id", "series"]]   
        
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
                    if book in books_out[bid]:
                        books_out[bid][book].append(clusters)
                    else:
                        books_out[bid][book] = clusters
                else:
                    books_out[bid] = {book : clusters}
            
           
            print("Getting books by clusters")
            # Filter by clusters and write books for each cluster to dict
            for cluster in tqdm(unique_c):
                books = data.loc[data["cluster"] == cluster]["id"].values.tolist()
                if cluster in clusters_out:
                    
                    clusters_out[cluster].extend(books)
                    
                else:
                    clusters_out[cluster] = books
    
    # Once all files have been processed write the dictionaries to json at specified locations
    books_json = json.dumps(books_out, indent = 2)
    clusters_json = json.dumps(clusters_out, indent = 1)
    
    with open(books_out_path, 'w') as f:
        f.write(books_json)
        f.close()
    
    with open(clusters_out_path, 'w') as f:
        f.write(clusters_json)
        f.close
    
    # Return the dictionaries in case additional work is needed
    return books_out, clusters_out
        
path_in = "D:/Corpus Stats/2021/Cluster data/Oct_2021/parquet"
p_out_books = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/books.json"
p_out_clusters = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/clusters.json"

books, clusters = parquet_books_clusters(path_in, p_out_books, p_out_clusters)                      
                
        

