# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 15:39:39 2022

@author: mathe
"""

## A quick fix for data written incorrectly for the app - using a deprecated script

import json

json_in = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/books.json"
out_path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/for_ms_app/books_dict.json"

with open(json_in) as f:
    data = json.load(f)
    f.close()

out_dict = {}

for book in data.keys():
    listed = data[book]
    ms_dict = {}
    for ms in listed:
        ms_id = ms.keys()
        for key in ms_id:
            ms_dict[key] = ms[key]
        out_dict[book] = ms_dict

json_data = json.dumps(out_dict, indent = 2)

with open(out_path, "w") as f:
    f.write(json_data)
    f.close()