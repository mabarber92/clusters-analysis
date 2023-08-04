import pandas as pd
from tqdm import tqdm

def update_cls_vers_ids(cluster_df, new_ids_paths, drop_prev_ids = True, id_splitter = "Sec"):
    """new_id_paths is a list of dictionaries giving fields "new_ids" "new_ids_meta", where both supply absolute
     paths to the csv files for the changes. Example:
     [{"new_ids": "/0845MaqriziRasaili.cluster-section-ids.csv",
        "new_ids_meta": "//0845MaqriziRasaili.section-ids-meta.csv"
        }]
     
     cluster_df should be the clusters with light metadata (it needs to have the 'book' field for the script to work)
     
     The new_ids_metadata is merged with the new_ids so that the book field is migrated - for easily adding to the metadata file

     The script loops through the new_ids, identifying the clusters within it and creating new rows with the new_id and book.
     If drop_prev_ids is True, then the existing version ids in the cluster data will be dropped (corresponding only to clusters in the input - that is, if there are clusters in the cluster
     data that are not in the new_ids data then those rows will not be dropped) - this ensures that there is not a repetition of data. 
     Will be set depending on what kind graph you want to draw. Sometimes it will be useful to have the reuse for the existing complete ID and
     for the new sections all on the same graph - sometimes not.
     
     id_splitter is the string within the new id that separates the old Id from the new unqiue identified (for example related to the sections
     of the book)
     
     """
    # Check the cluster df has the book field
    if "book" in cluster_df.columns:        
        book_field = True
    else:
        print("No book column in the cluster_df - merging only ids")
        book_field = True

    # Initiate the DataFrame containing the new rows and the list for the dropped indexes
    new_rows = pd.DataFrame()
    idxs_to_drop = []

    # We loop through the list of csvs for the changes - allows us to implement multiple id changes in one go
    for files_dict in new_ids_paths:
        print("Creating id rows for: {}".format(files_dict))
        
        # Load in the new data
        new_ids_meta = pd.read_csv(files_dict["new_ids_meta"], encoding='utf-8-sig')[["id", "book"]]
        new_ids = pd.read_csv(files_dict["new_ids"])

        # Add the metadata to the new_ids
        new_ids = pd.merge(new_ids, new_ids_meta, on="id")

        # Transform new_ids into a list of dicts for iteration - get ids from first item 
        new_ids = new_ids.to_dict("records")
        vers_id = new_ids[0]["id"].split(id_splitter)[0]
        print(vers_id)
        
        # Create a smaller filtered df to with only the ids
        pre_filtered_df = cluster_df[cluster_df["id"] == vers_id]

        # Loop, identify corresponding roles in the clusters and add to new_rows and indexes to drop to idxs_to_drop
        for new_id in tqdm(new_ids):            
            filtered_cls = pre_filtered_df[pre_filtered_df["cluster"] == new_id["cluster"]]            
            filtered_cls["id"] = new_id["id"]
            if book_field:
                filtered_cls["book"] = new_id["book"]
            
            new_rows = pd.concat([new_rows, filtered_cls])
            idxs_to_drop.extend(filtered_cls.index.tolist())
    
    # If set to true, use the indexs of the changed rows to drop them from the existing cluster_df
    if drop_prev_ids:
        print("Dropping existing id rows - {} total rows dropped".format(len(idxs_to_drop)))
        cluster_df = cluster_df.drop(idxs_to_drop)
    
    # Add the new rows to the cluster_df
    cluster_df = pd.concat([cluster_df, new_rows])

    return cluster_df
