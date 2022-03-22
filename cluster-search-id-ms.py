# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 10:02:47 2022

@author: mathe
"""
import pandas as pd
import pyarrow.parquet as pq
import os
from tqdm import tqdm

def search_id_ms(bid_mls, parquet_in, out_path):
    os.chdir(parquet_in)
    out_df = pd.DataFrame()
    for root, dirs, files in os.walk(".", topdown=False):
        for name in tqdm(files):
            pq_path = os.path.join(root, name)            
            data = pq.read_table(pq_path).to_pandas()[["cluster", "size", "id", "text"]]
            for bid_ml in bid_mls:
                temp_df = data[data["id"] == bid_ml]
                out_df = pd.concat([out_df, temp_df])
    out_df.to_csv(out_path, index = False, encoding = 'utf-8-sig')
    


ids = ["JK001330-ara1.mARkdown.ms0017", "JK000911-ara1.mARkdown.ms0810", "JK001330-ara1.mARkdown.ms0018", "JK000911-ara1.mARkdown.ms0811"]
parquet_in = "D:/Corpus Stats/2021/Cluster data/Oct_2021/parquet/"
path_out = "C:/Users/mathe/OneDrive/Documents/Conferences and Papers/Graphs/data/example_clusters.csv"

search_id_ms(ids, parquet_in, path_out)