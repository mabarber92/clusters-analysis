# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 09:26:24 2022

@author: mathe
"""
import pandas as pd
import pyarrow.parquet as pq
import os
from tqdm import tqdm
import seaborn as sns

def map_cluster_sizes(parquet_path, figure_out, spec_stats = [2], bins_no = 20, cl_thres = 20):
    """Function that maps out the distribution of clusters in a Seaborn histogram,
    gives the total cluster count, and statistics ('spec_stats') for clusters of a specified
    size"""
    # Create a df of all the clusters and their sizes
    final_df = pd.DataFrame()
    os.chdir(parquet_path)
    for root, dirs, files in os.walk(".", topdown=False):
        for name in tqdm(files):
            pq_path = os.path.join(root, name)            
            data = pq.read_table(pq_path).to_pandas()[["cluster", "size"]]
            final_df = pd.concat([final_df, data])
    # Ensuring the column we use for stats is numeric
    final_df = final_df.drop_duplicates()        
    final_df["size"] = pd.to_numeric(final_df["size"])
    
    total_c = len(final_df)
    
    # Dropping rows above threshold
    idxnames = final_df[final_df["size"] > cl_thres].index
    final_df.drop(idxnames, inplace = True)
    print(final_df["size"].max())
    
    print("getting stats")    
    
    out_stats = [["Total clusters", total_c, 100]]
    over_thres_c = len(idxnames)
    over_thres_p = (over_thres_c/total_c) *100
    out_stats.append(["Clusters above size - " + str(cl_thres), over_thres_c, over_thres_p])
    for count in spec_stats:
        subset_df = final_df.loc[final_df["size"] == count]
        sub_c = len(subset_df)
        sub_p = (sub_c/total_c)*100
        out_stats.append(["Clusters of size - " + str(count), sub_c, sub_p])
    
    stats_df = pd.DataFrame(out_stats, columns = ["Type", "Count", "Percentage"])
    print(stats_df)
        
    
    print("Creating histogram")
    # Produce a historgram 
    g = sns.histplot(final_df, x = "size", bins = bins_no)
    fig = g.get_figure()
    fig.savefig(figure_out)
    
    return stats_df, g

path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/parquet/"
image_out = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/cluster_hist.png"

stats, g = map_cluster_sizes(path, image_out, spec_stats = [2,3,4,5,6,7,8,9,10])
    