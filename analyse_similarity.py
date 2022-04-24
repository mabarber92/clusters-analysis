# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 11:42:00 2022

@author: mathe
"""

from collections import Counter
import pandas as pd
from tqdm import tqdm

def analyse_similarity(clusters_df, word_cap, cluster_size, grams = 2):
    # Get list of unique clusters through which to fetch clusters and analyse strings
    all_cls = clusters_df["cluster"].tolist()
    cl_nos = list(dict.fromkeys(all_cls))
    print("Total clusters: " + str(len(cl_nos)))
    
    df_out = pd.DataFrame()
    passed_clusters = 0
    
    # Loop through the clusters and see if they meet criteria
    for cl_no in tqdm(cl_nos):
        cluster_df = clusters_df[clusters_df["cluster"] == cl_no]
        strings_list = cluster_df["text"].tolist()
        
        # If working with ngrams - no need to shingle each string separately
        if grams == 1:
            gram_bag = " ".join(strings_list).split(" ")
        
        # Else apply shingling by specified ngram
        else:
            gram_bag = []
            for string in strings_list:
                string_toks = string.split(" ")
                string_len = len(string_toks)
                for pos, tok in enumerate(string_toks):
                    # For 2-gram - get a subset based on position - to create shingle
                    if pos != string_len - grams:
                        new_gram = " ".join(string_toks[pos:pos+grams])
                        if new_gram != " ":
                            gram_bag.append(new_gram)
        
        # Count the gram bag and single out clusters that have lower than expected no.
        count_list = []
        grams_counted = Counter(gram_bag)
        for key in grams_counted.keys():
            count_list.append({"word" : key, "count" : grams_counted[key]})
        if cl_no == 85899491236:
            pd.DataFrame(count_list).sort_values(["count"], ascending=False).reset_index(drop=True).to_csv("C:/Users/mathe/Documents/Github-repos/clusters-analysis/2-gram-check.csv", encoding = 'utf-8-sig')
        selected_row = pd.DataFrame(count_list).sort_values(["count"], ascending=False).reset_index(drop=True).loc[word_cap]        
        selected_count = selected_row["count"].tolist()
        if selected_row["count"] < cluster_size:            
            passed_clusters = passed_clusters + 1
            cluster_df["max_count"] = selected_count
            df_out = pd.concat([df_out, cluster_df])
    
    df_out = df_out.sort_values(["max_count"])
    print("Clusters that meet criteria:" + str(passed_clusters))
    return df_out

path = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/clusters-by-size/clusters-size-15.csv"
cl_df = pd.read_csv(path, encoding = 'utf-8-sig')
output = analyse_similarity(cl_df, 3, 15)