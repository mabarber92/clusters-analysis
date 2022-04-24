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
import matplotlib.pyplot as plt

def map_cluster_sizes(parquet_path, figure_out, spec_stats = [2], cl_thres = 20, full_hist = None, all_clusters = None):
    """Function that maps out the distribution of clusters in a Seaborn histogram,
    gives the total cluster count, and statistics ('spec_stats') for clusters of a specified
    size"""
    bins_no = cl_thres - 2
    # Create a df of all the clusters and their sizes
    if all_clusters is None :
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
    else:
        final_df = all_clusters
    total_c = len(final_df)
    
    # If full_hist is selected produce a hist on everything but the largest cluster (that is
    # cluster under size 1000)
    if full_hist is not None:
        sns.set_style("whitegrid")
        yticks = [1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 2000000]
        xticks = [2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000 ]
        # print ("Filtered for full hist")
        # filtered_df = final_df[final_df["size"] < 1000]
        
        print("creating full hist")
        gfull = sns.histplot(final_df, x = "size", bins = 50, log_scale = True)
        gfull.set_yscale("log")

        gfull.set_yticks(yticks)
        gfull.set_yticklabels(yticks)
        gfull.set_xticks(xticks)
        gfull.set_xticklabels(xticks, rotation = 90)
        gfull.set_xlabel("Cluster Size")
        fig_full = gfull.get_figure()
        fig_full.savefig(full_hist, dpi = 300, bbox_inches = 'tight')
        plt.clf()
        
    # Dropping rows above threshold
    # idxnames = final_df[final_df["size"] > cl_thres].index
    # out_df = final_df.copy()
    # final_df.drop(idxnames, inplace = True)
    # print(final_df["size"].max())
    
    print("getting stats")    
    
    out_stats = []
    
    
    for count in range(2, cl_thres + 1):
        subset_df = final_df.loc[final_df["size"] == count]
        sub_c = len(subset_df)
        sub_p = (sub_c/total_c)*100
        out_stats.append([str(count), sub_c, sub_p])    
    over_thres_c = len(final_df.loc[final_df["size"] > cl_thres])
    over_thres_p = (over_thres_c/total_c) *100
    out_stats.append([">" + str(cl_thres), over_thres_c, over_thres_p])
    out_stats.append(["Total clusters", total_c, 100])
    stats_df = pd.DataFrame(out_stats, columns = ["Cluster Size", "Count", "Percentage"])
    print(stats_df)
        
    
    # print("Creating histogram")
    # # Produce a historgram
    # listed_ticks = []
    # for i in range(2, cl_thres + 1):
    #     listed_ticks.append(i)
    # g = sns.histplot(final_df, x = "size", bins = bins_no)
    # g.set_xticks(listed_ticks)
    # fig = g.get_figure()
    # fig.savefig(figure_out, dpi = 300, bbox_inches = 'tight')
    
    ticks = [500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 2000000]
    sns.set_theme(style = "whitegrid")
    g = sns.barplot(data = stats_df[:-1], x = "Cluster Size", y = "Count", color = "lightblue")
    g.set_yscale("log")
    g.set_yticks(ticks)
    g.set_yticklabels(ticks)
    fig = g.get_figure()
    fig.savefig(figure_out, dpi = 300, bbox_inches = 'tight')
    
    return stats_df, g, gfull, final_df

path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/parquet/"
image_out = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/cluster_bar2.png"
full_fig_out = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/full_cluster_hist_logxy.png"

# stats, g, gfull, all_clusters = map_cluster_sizes(path, image_out, full_hist = full_fig_out)
    