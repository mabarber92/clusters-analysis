# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 10:53:41 2022

@author: mathe
"""
import pandas as pd
import json

def find_list_cluster(cluster_ids, cluster_dict, meta_path, out_path):
    if type(cluster_ids) == str or type(cluster_ids) == int:
        cluster_ids = [cluster_ids]
    meta_df = pd.read_csv(meta_path, sep="\t")[["id", "book"]]
    for clid in cluster_ids:
        bids_ms = cluster_dict[str(clid)]
        out = pd.DataFrame()
        bids = []
        for bid_ms in bids_ms:
            bid = bid_ms.split("-")[0]
            bids.append(bid)
        out["id-ms"] = bids_ms
        out["id"] = bids        
        out = pd.merge(out, meta_df, on="id", how= "inner")
        out = out.drop(columns = ["id"])
        csv_path = out_path + "/book_list-cl-" + clid + ".csv"
        out.to_csv(csv_path, index=False)

cluster_path = "C:/Users/mathe/Documents/Kitab project/Visualisations/cluster_ms_book/data/Oct_2021/clusters.json"
meta_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5_merged_wNoor.csv"
out = "C:/Users/mathe/OneDrive/Documents/Conferences and Papers/Graphs/data/"

with open(cluster_path) as f:
    clusters_data = json.load(f)
    f.close()
    
cluster = "180388738305"
    
find_list_cluster(cluster, clusters_data, meta_path, out)