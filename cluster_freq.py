# -*- coding: utf-8 -*-
"""
Script to count frequencies of Clusters and sort by size
"""

import pandas as pd
from tqdm import tqdm
import os
import json

def count_clusters(cluster_path, max_freq, out, meta):
    final_df = pd.DataFrame()
    metadata = pd.read_csv(meta, sep= "\t")[["book", "id"]]
    os.chdir(cluster_path)
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            json_path = os.path.join(root, name)
            print(json_path)
            print("Getting data")
            data = pd.read_json(json_path, lines = True)[["cluster", "series"]]
            
            print("Concatenating data")
            final_df = pd.concat([final_df, data])
            

    
    print("Getting frequencies...")
    # counts = final_df[["cluster"]].value_counts()
    
    print("Count freqs per cluster")
    clusters = list(dict.fromkeys(data["cluster"]))
    new_listed = []
    for cluster in tqdm(clusters):
        subset = final_df.loc[final_df["cluster"] == cluster]
        count_clusters= len(subset)
        books = list(dict.fromkeys(subset["series"]))
        count_books = len(books)
        new_listed.append([cluster, count_clusters, count_books])
        
        if count_clusters > max_freq:
            print("Cluster over: " + str(max_freq))
            cluster_df_listed = []
            for book in tqdm(books):
                sub_subset = subset.loc[subset["series"] == book]
                bookmeta = metadata.loc[metadata["id"] == book.split("-")[0]].values.tolist()
                
                count = len(sub_subset)
                if len(bookmeta) == 0:
                    book_name = "Not found"
                else:
                    book_name = bookmeta[0][0]
                
                cluster_df_listed.append([book, book_name, count])
            df_book = pd.DataFrame(cluster_df_listed, columns = ["Book", "Meta", "Count"])
            df_book.to_csv(out + "cluster" + str(cluster) + ".csv", index = False)
    
    df_clusters = pd.DataFrame(new_listed, columns = ["Cluster", "Ms_count", "Book_count"])
    
    df_clusters.to_csv(out + "cluster_freqs.csv", index = False)



json_path = "D:/Corpus Stats/2021/Cluster data/full_clusters/out.json/"
out = "C:/Users/mathe/Documents/Kitab project/Cluster_anal/freq_out/"
# meta_dict_path = "C:/Users/mathe/Documents/Kitab project/Visualisations/Cluster_MS_dash/data/id_meta_pair.json"
# with open(meta_dict_path) as f:
#     meta_dict = json.load(f)
#     f.close
meta = "C:/Users/mathe/Documents/Kitab project/Visualisations/Resource_scripts/OpenITI_metadata_2021-1-4.csv"
counts = count_clusters(json_path, 500, out, meta)