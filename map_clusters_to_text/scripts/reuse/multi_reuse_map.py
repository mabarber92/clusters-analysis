# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 10:48:42 2022

@author: mathe
"""

"""Add ability to get section offsets within the function - to improve overall
accuracy of offset matches"""


import re
import os
import pandas as pd
from reuse.pattern_mapping_cat import pattern_map_dates

def multi_reuse_map(all_cls, tagged_text_path, dir_out, reused_texts = [], section_map = False, date_summary=None, tops = None, date_cats = [], other_map = False):
    """For reused texts supply just 0000author.book URIs - the selection of all_cls will
    determine which version is used"""
    # Get filename
    text_name = tagged_text_path.split("\\")[-1]
    print(text_name)

    
    
    # Get cluster locs from the text
    with open(tagged_text_path, encoding = "utf-8-sig") as f:
        tagged_text = f.read()
        f.close()
    
    print("getting cluster locs")
    cluster_locs = re.finditer(r"@cl([be])@?\d*@(\d+)@", tagged_text)
    cluster_offsets = {}
    for cluster in cluster_locs:
        cluster_no = cluster.group(2)
        if cluster_no not in cluster_offsets.keys():
            cluster_offsets[cluster_no] = {}
        if cluster.group(1)== "b":
            cluster_offsets[cluster_no]["ch_start_tar"] = cluster.start()
        if cluster.group(1) == "e":
            cluster_offsets[cluster_no]["ch_end_tar"] = cluster.start()
        
    
    print("cluster offsets calculated")
    
    # Fetch clusters and create compressed data for selected reused texts
    filtered_clusters = pd.DataFrame()
    if reused_texts == []:
        print("Excluding main text:" + ".".join(text_name.split(".")[0:2]))
        filtered_clusters = all_cls[all_cls["book"] != ".".join(text_name.split(".")[0:2])]
    else:
        if other_map:
            remaining_clusters = all_cls[all_cls["book"] != ".".join(text_name.split(".")[0:2])]
        for reused_text in reused_texts:
            
            book_data = all_cls[all_cls["book"] == reused_text]

            filtered_clusters = pd.concat([filtered_clusters, book_data])
            if other_map:
                remaining_clusters = remaining_clusters[remaining_clusters["book"] != reused_text]
      

    # Loop through clusters and create the df
    reuse_map_out = []
    for cluster in cluster_offsets.keys():
        
        cluster_data = filtered_clusters[filtered_clusters["cluster"] == int(cluster)][["book", "seq", "id"]].values.tolist()
        cluster_start = cluster_offsets[cluster]["ch_start_tar"]
        cluster_end = cluster_offsets[cluster]["ch_end_tar"]

        for book in cluster_data:
            
            reuse_map_out.append({"Text": book[0], "id": book[2], "ch_start_tar":cluster_start, "ch_end_tar":cluster_end, "source_book_ms": book[1], "book_count": 1, "cluster": cluster})
        if reused_texts != [] and other_map:
            cluster_data = remaining_clusters[remaining_clusters["cluster"] == int(cluster)][["book", "seq"]].values.tolist()
            cluster_start = cluster_offsets[cluster]["ch_start_tar"]
            cluster_end = cluster_offsets[cluster]["ch_end_tar"]
            
            if len(cluster_data) > 0:            
                reuse_map_out.append({"Text": "Other", "id": "n/a", "ch_start_tar":cluster_start, "ch_end_tar":cluster_end, "source_book_ms": 0, "book_count": len(cluster_data), "cluster": cluster})
    
    reuse_out = pd.DataFrame(reuse_map_out)
    
    
    reuse_out_path = dir_out + "/" + text_name + "-reuse.csv"        
    reuse_out.to_csv(reuse_out_path)
    

    if section_map:
        print("Creating section map")
        # Add code here for section map - output to the same directory
        section_df = pattern_map_dates(tagged_text, char_counts = True, tops = tops, date_summary=date_summary, date_cats = date_cats)
        section_out_path = dir_out + "/" + text_name + "-section.csv"   
        section_df.to_csv(section_out_path)
        
def multi_reuse_map_corpus(all_cls, tagged_text_dir, dir_out, reused_texts = [], section_map = False, date_summary=None, tops = None, date_cats = [], other_map = True):
    for root, dirs, files in os.walk(tagged_text_dir):
        for name in files:
            text_path = os.path.join(root, name)            
            multi_reuse_map(all_cls, text_path, dir_out, reused_texts = reused_texts, section_map = section_map, date_summary=date_summary, tops = tops, date_cats = date_cats, other_map = other_map)
