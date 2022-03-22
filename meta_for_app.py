# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 11:20:44 2022

@author: mathe
"""

import pandas as pd
import json

def create_meta(zenodo_meta, dash = True):
    metadata = pd.read_csv(zenodo_meta, sep = "\t")
    metadata = metadata.loc[metadata["status"] == "pri"]
    metadata = metadata[["author_lat", "title_lat", "local_path"]].values.tolist()
    if dash:
        dash_dict = []
        pair_dict = {}
        for row in metadata:
            title = row[0] + " , " + row[1]
            file_name = row[2].split('/')[-1]
            full_id = file_name.split('.')[2:]
            if type(full_id) == list:
                full_id = ".".join(full_id)
            dash_dict.append({"label": title, "value": full_id})
            pair_dict[full_id] = title
        return dash_dict, pair_dict

meta_df_path = "D:/Corpus Stats/2021/OpenITI_metadata_2021-2-5.csv"
labels_path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/meta.json"
meta_path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/id_meta_pair.json"    

labels, meta = create_meta(meta_df_path)

labels_js = json.dumps(labels, indent = 1)
meta_js = json.dumps(meta, indent = 1)

with open(labels_path,'w') as f:
    f.write(labels_js)
    f.close()
    
with open(meta_path,'w') as f:
    f.write(meta_js)
    f.close()

