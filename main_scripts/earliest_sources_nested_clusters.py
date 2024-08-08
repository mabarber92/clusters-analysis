# tag_clusters takes df not object - pass a cluster df to this
from cls_text_tag import tag_clusters
from clusterDf import clusterDf
import pandas as pd
from tqdm import tqdm



def level_nests(list_dict):
    nested_levels_dict = []
    # If there is only one cluster in the ms - produce an blank output with no levels
    if len(list_dict) == 1:
        parent_length = list_dict[0]["end"] - list_dict[0]["begin"]
        row = {"parent_cl": list_dict[0]["cluster"], "par_begin": list_dict[0]["begin"], "par_len": parent_length, "level_1": 0, "per_of_par": 0, "level_2": 0, "per_of_l1" : 0, "level_3": 0, "per_of_l2": 0}
        nested_levels_dict.append(row)
    
    #Otherwise follow the levelling procedure
    else:
        # Create copy of list
        list_dict_copy = list_dict[:]

        # Create store for nested clusters
        nested_clusters = {}
        drop_ids = []

        # Loop through list 1 and find nestings in list 2 - create a list of ids to drop
        for cluster in list_dict:
            begin = cluster["begin"]
            end = cluster["end"]
            length = end - begin
            id = cluster["cluster"]
            nested_clusters[id] = {"begin": begin, "length": length, "clusters" : []}
            for cluster2 in list_dict_copy:
                id2 = cluster2["cluster"]
                begin2 = cluster2["begin"]
                end2 = cluster2["end"]
                if id2 == id:
                    continue
                elif cluster2["begin"] >= begin and cluster2["end"] <= end:
                    length2 = end2 - begin2
                    cl_field = {"cluster": id2, "length": length2}
                    nested_clusters[id]["clusters"].append(cl_field)
                    # if cluster2["begin"] == begin or cluster2["end"] == end:
                    #     drop_ids.append(id2)
        
        # Rationalise into a deeper nest - Note clear what the levelling is really doing here
        # Q. How do we document earliest source in larger pieces and within the nests - in a way that will help us visualise what sources are being used
        levels_dict_list = []
        # skip_list = []
        # for item in nested_clusters.keys():
        #     if item in skip_list:
        #         continue
        #     row = {"parent_cl": item, "level_1": [], "level_2": [], "level_3": []}
        #     nested_list = nested_clusters[item]        
        #     for cl in nested_list:
        #         if cl in skip_list:
        #             continue
        #         level2_list = nested_clusters[cl]
        #         if len(level2_list) > 0:
        #             # Then it has a second level              
        #             for cl2 in level2_list:
        #                 if cl2 in skip_list:
        #                     continue
        #                 level3_list = nested_clusters[cl2]                    
        #                 if len(level3_list) > 0:
        #                     #Then it has a third level
        #                     row["level_3"] = level3_list
        #                     skip_list.extend(level3_list)
        #                 skip_list.append(cl2)
        #                 row["level_2"].append(cl2)                
        #         row["level_1"].append(cl)
        #         skip_list.append(cl)
            
        
        skip_list = []
        # Test code
        log = False

        for item in nested_clusters.keys():
            
            if item in skip_list:
                continue
            
            nested_list = nested_clusters[item]["clusters"]
            parent_length = nested_clusters[item]["length"]        
            for cl in nested_list:
                
                if not cl["cluster"] in skip_list:
                    l1_length = cl["length"]    
                    row = {"parent_cl": item, "par_begin": nested_clusters[item]["begin"], "par_len": parent_length, "level_1": cl["cluster"], "per_of_par": round(l1_length/parent_length*100, 2), "level_2": 0, "per_of_l1" : 0, "level_3": 0, "per_of_l2": 0}
                    skip_list.append(item)
                    skip_list.append(cl["cluster"])
                            
                    level2_list = nested_clusters[cl["cluster"]]["clusters"]
                    if len(level2_list) > 0:
                        
                        # Then it has a second level              
                        for cl2 in level2_list:
                            if not cl2["cluster"] in skip_list:
                                l2_length = cl2["length"]    
                                row["level_2"] = cl2["cluster"]
                                row["per_of_l1"] = round(l2_length/l1_length*100, 2)
                                skip_list.append(cl2["cluster"])
                                
                                level3_list = nested_clusters[cl2["cluster"]]["clusters"]                    
                                if len(level3_list) > 0:
                                    # Test code for larger l3s
                                    # if len(level3_list) > 1:
                                    #     log = True                        
                                    #Then it has a third level
                                    for cl3 in level3_list:
                                        if not cl3["cluster"] in skip_list:
                                            l3_length = cl3["length"]
                                            row["level_3"] = cl3["cluster"]
                                            row["per_of_l2"] = round(l3_length/l2_length*100, 2)
                                            skip_list.append(cl3["cluster"])
                                            
                                            if row not in nested_levels_dict:
                                                nested_levels_dict.append(row)
                                            
                                
                                if row not in nested_levels_dict:
                                    nested_levels_dict.append(row)
                                
                    if row not in nested_levels_dict:
                        nested_levels_dict.append(row)              
                


    

    # print(list_dict)
    # print(nested_levels_dict)
    levels_df = pd.DataFrame(nested_levels_dict).astype(str)
    # print(levels_df)

    return levels_df

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


    # print(list_dict)
    # print(nested_clusters)
    # print(levels_df)

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





def level_nested_clusters(book_cluster_df):
    """Take a cluster_df for a cluster dataframe for one book - it must be one book for this to work - and
    create a dataframe documenting how individual clusters are nested into one another and percentages of a cluster that a
    nested cluster occupies"""

    out_df = pd.DataFrame()
    # For each ms (seq) sort by begin, then end, - transform into list of dicts - and level the nests
    ms_list = book_cluster_df["seq"].drop_duplicates().to_list()
    for ms in tqdm(ms_list):
        ms_clusters = book_cluster_df[book_cluster_df["seq"] == ms]
        ms_clusters = ms_clusters.sort_values(by=["begin", "end"], ascending=[True, False]).to_dict("records")
   
        #
        nested_levels = level_nests(ms_clusters)
        
        nested_levels["ms"] = ms            
        out_df = pd.concat([out_df, nested_levels])
            
    return out_df

def leveled_df_unique_cls(leveled_df, cols = ["parent_cl", "level_1", "level_2", "level_3"]):
    """Function takes the default columns containing cluster numbers and reduces them to a unique list of clusters"""
    cluster_list = []
    test_list = []
    for col in cols:
        cluster_list.extend(leveled_df[col].drop_duplicates().to_list())
        test_list.extend(leveled_df[col].to_list())
    cluster_list = list(dict.fromkeys(cluster_list))
    print(len(cluster_list))
    print(len(test_list))
    return cluster_list

def create_earliest_source_lookup(clusters, cluster_df, current_book):
    """Take a cluster df, lookup each cluster and find the earliest source for each cluster
    **Current config:** A cluster where there are multiple min death dates we output a list of
    all books with that death date"""
    # Simplify cluster_df to speed-up
    cluster_df = cluster_df[["cluster", "book", "date"]]
    
    # Drop the book being studied from the data
    cluster_df = cluster_df[cluster_df["book"] != current_book]

    #Initiate the lookup dict
    lookup_dict = {}

    # Populate the lookup dict - remember to return None where the returned df is empty (that is we have a late source filtered from the data)
    for cluster in tqdm(clusters):
        filtered_df = cluster_df[cluster_df["cluster"] == int(cluster)]
        if len(filtered_df) > 0:
            min_date = min(filtered_df["date"].to_list())
            min_books = []
            for row in filtered_df.to_dict("records"):
                if row["date"] == min_date:
                    min_books.append(row["book"])
                    min_books = list(dict.fromkeys(min_books))
        else:
            min_books = "No Data"
        lookup_dict[cluster] = min_books
    
    return lookup_dict
    

def lookup_earliest_sources(leveled_df, source_dict):
    leveled_dict = leveled_df.to_dict("records")

    # Loop through rows of the df, lookup clusters and add them
    for row in tqdm(leveled_dict):
        for col in ["parent_cl", "level_1", "level_2", "level_3"]:
            cluster = row[col]
            if cluster != '0':
                data = source_dict[cluster]
            else:
                data = "No Cluster"
            row['{}_book'.format(col)] = data
    
    df_out = pd.DataFrame(leveled_dict)
    return df_out


def earliest_sources_nested_clusters(cluster_path, meta_path, book):
    """Overall processs for transforming clusters into a dataframe that records nesting and the earliest available reused in each
    cluster for each nested level"""

    # Create a cluster obj
    cluster_obj = clusterDf(cluster_path, meta_path)

    # Get a df for only clusters related to the primary book
    cluster_df = cluster_obj.return_cluster_df_for_uri_ms(book)
    

    # Pass a df with only the main book for the nesting
    leveled_df = level_nested_clusters(cluster_df[cluster_df["book"] == book])
    

    # Using unique cluster list from leveled_df create a look-up dict for earliest source for each cluster
    cluster_list = leveled_df_unique_cls(leveled_df)
    source_dict = create_earliest_source_lookup(cluster_list, cluster_df, book)

    # Lookup the earliest books using the leveled_df
    sources_df = lookup_earliest_sources(leveled_df, source_dict)

    sources_df = sources_df.sort_values(by=["ms", "par_begin"])
    sources_df.to_csv("{}_leveled_clusters_earliest_source.csv".format(book), index=False)
    
    

if __name__ == "__main__":
    cluster_path = "D:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    book = "0845Maqrizi.Mawaciz"


    earliest_sources_nested_clusters(cluster_path, meta_path, book)
    