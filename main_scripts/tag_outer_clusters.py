# tag_clusters takes df not object - pass a cluster df to this
from cls_text_tag import tag_clusters
from clusterDf import clusterDf
import pandas as pd

def sort_dict_len(sub):
    return len(sub)

def remove_nests(list_dict, output = [], idx = 0):

    # Create copy of list
    list_dict_copy = list_dict[:]

    # Create store for nested clusters
    nested_clusters = {}
    drop_ids = []

    # Loop through list 1 and find nestings in list 2 - create a list of ids to drop
    for cluster in list_dict:
        begin = cluster["begin"]
        end = cluster["end"]
        id = cluster["index"]
        nested_clusters[id] = []
        for cluster2 in list_dict_copy:
            id2 = cluster2["index"]
            if id2 == id:
                continue
            elif cluster2["begin"] >= begin and cluster2["end"] <= end:
                nested_clusters[id].append(id2)
                drop_ids.append(id2)
    
    # Rationalise into a deeper nest - Note clear what the levelling is really doing here
    # Q. How do we document earliest source in larger pieces and within the nests - in a way that will help us visualise what sources are being used
    levels_dict = {"level 1" : [], "level 2": [], "level 3": []}
    skip_list = []
    for item in nested_clusters.keys():
        if item in skip_list:
            continue
        nested_list = nested_clusters[item]       
        for cl in nested_list:
            if cl in skip_list:
                continue
            level2_list = nested_clusters[cl]
            if len(level2_list) > 0:
                # Then it has a second level                
                for cl2 in level2_list:
                    if cl2 in skip_list:
                        continue
                    level3_list = nested_clusters[cl2]
                    skip_list.append(cl2)
                    levels_dict["level 2"].append(cl2)
                    if len(level3_list) > 0:
                        #Then it has a third level
                        levels_dict["level 3"].extend(level3_list)
                        skip_list.extend(level3_list)
                        
            skip_list.append(cl)
            levels_dict["level 1"].append(cl)

        # for level2_cl in nested_list:
        #     level2_list = nested_clusters[level2_cl]
        #     if len(level2_list) > 0:
        #         for level3_cl in level2_list:
        #             level3_list = nested_clusters[level3_cl]
        #             if len(level3_list) > 0:
        #                 levels_dict["level 3"].append(level3_list)
        #                 level2_list.remove(level3_cl)
        #         levels_dict["level 2"].append(level2_list)
        #         nested_list.remove(level2_cl)
        # levels_dict["level 3"].append(nested_list)
    
    # list_out = []
    # # Filter out drop ideas
    # for cluster in list_dict:
    #     if cluster["index"] not in drop_ids:
    #         list_out.append(cluster)


    print(list_dict)
    print(nested_clusters)
    print(levels_dict)

    # Simply resulting list
    # Sort by length of lists
 


    # Factor out from largest to smallest - creating list of those to keep
    

    """Principles of this process as follows: given a list of clusters sorted by start point, then end point, if for a cluster at a given index, the following 
     cluster in the list start before the end point and end before the end point, then it is considered nested, we loop through subsequent rows
      if we find a cluster that starts or ends after the end point of the current cluster, then we store the parent cluster in output and
      repeat the process starting at the index of the cluster that failed the criteria. This continues until the list is complete. The end of 
      the list is the criterion for ending recursion and returning the accumulated output"""
    
    # If the index is greater than the length of the list then we've completed the recursion and can return the output
    # if idx >= len(list_dict):
    #     print(output)
    #     return output
    # # Otherwise take the current position in the list (based on the previous iteration) and skip forward over anything that is nested within that cluster
    # else:
        

        
    # if idx == len(list_dict) - 1:
    #     return output
    # else:
    #     select_row = list_dict[idx]
    #     print(select_row)
    #     end = select_row['end']
    #     for item in list_dict[idx+1:]:
    #         if item["begin"] > end or item["end"] > end:
                
    #             output.append(select_row)
    #             remove_nests(list_dict, output, item["index"])





def filter_out_nested_spans(book_cluster_df):
    """Take a cluster_df for a cluster dataframe for one book - it must be one book for this to work - and
    filter out clusters that are clearly nested within the spans of other clusters"""

    full_output = []

    # For each ms (seq) sort by begin, then end, - transform into list of dicts - and recurse on ms
    ms_list = book_cluster_df["seq"].drop_duplicates().to_list()
    for ms in ms_list[0:5]:
        ms_clusters = book_cluster_df[book_cluster_df["seq"] == ms]
        ms_clusters = ms_clusters.sort_values(by=["begin", "end"], ascending=[True, False]).to_dict("records")

        indexed_list = []
        for idx, cluster in enumerate(ms_clusters):
            cluster["index"] = idx
            indexed_list.append(cluster)
        
        if len(indexed_list) == 1:
            
            full_output.extend(indexed_list)
        
        else:
            
            # Recursive function - go through and store indexes to keep
            print(indexed_list)
            unnested = remove_nests(indexed_list)
            # print(unnested)
            
            # Extend output using results
            # full_output.extend(unnested)
            

    # Trial code to test output
    df = pd.DataFrame(unnested)
    df.to_csv("test_unnested.csv")

if __name__ == "__main__":
    cluster_path = "D:/Corpus Stats/2023/v7-clusters/minified_clusters_pre-1000AH_under500.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2022-2-7_merged.csv"
    book = "0845Maqrizi.Mawaciz"
    cluster_obj = clusterDf(cluster_path, meta_path)
    cluster_obj.filter_by_book_list([book])
    cluster_df = cluster_obj.cluster_df

    filter_out_nested_spans(cluster_df)
    