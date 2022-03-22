# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 11:20:44 2022

@author: mathe
"""

import pandas as pd
import json

def create_meta(zenodo_meta, dash = True):
    metadata = pd.read_csv(zenodo_meta, sep = "\t")
    
    metadata = metadata[["book", "id"]].values.tolist()
    if dash:        
        pair_dict = {}
        for row in metadata:
            title = row[0]
            pair_dict[row[1]] = title
        return pair_dict

meta_df_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5.csv"
meta_path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/id_meta_pair_clusters.json"    

meta = create_meta(meta_df_path)


meta_js = json.dumps(meta, indent = 1)


with open(meta_path,'w') as f:
    f.write(meta_js)
    f.close()

