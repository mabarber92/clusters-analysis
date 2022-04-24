# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 15:22:14 2022

@author: mathe
"""

import pandas as pd

def cluster_to_all_pairs(cluster_no, cluster_df, out_path):
    ids_strings = cluster_df[cluster_df["cluster"] == cluster_no][["id", "text"]]
    ids_strings = ids_strings.values.tolist()
    ids_strings_copy = ids_strings.copy()

    out_list = []
    for id_string in ids_strings:
        ids_strings_copy.remove(id_string)

        for id_string_copy in ids_strings_copy:
            row = [id_string[0]]
            row.append(id_string[1])
            row.append(id_string_copy[0])
            row.append(id_string_copy[1])

            out_list.append(row)

    out_df = pd.DataFrame(out_list, columns = ["id1", "s1", "id2", "s2"])
    out_df.to_csv(out_path, index = False, encoding = 'utf-8-sig')
    
    
# cluster_to_all_pairs(8590044369, output, "test_diff.csv")