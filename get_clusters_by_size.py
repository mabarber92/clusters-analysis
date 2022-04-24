import pandas as pd
import pyarrow.parquet as pq
import os
from tqdm import tqdm


def get_clusters_by_size(parquet_path, csv_path,  sizes = []):
    
    print("Fetching data")
    # Fetch and concatenate data
    final_df = pd.DataFrame()
    os.chdir(parquet_path)
    for root, dirs, files in os.walk(".", topdown=False):
        for name in tqdm(files):
            pq_path = os.path.join(root, name)            
            data = pq.read_table(pq_path).to_pandas()
            final_df = pd.concat([final_df, data])
    # Ensuring the column we use for stats is numeric
    final_df = final_df.drop_duplicates()        
    final_df["size"] = pd.to_numeric(final_df["size"])
    
    print("Creating csvs")
    # Find data by size and output to csv path
    for size in tqdm(sizes):
        filename = os.path.join(csv_path, "clusters-size-" + str(size) + ".csv")
        filtered_df = final_df[final_df["size"] == size]
        filtered_df.to_csv(filename, index = False, encoding = "utf-8-sig")

path = "D:/Corpus Stats/2021/Cluster data/Oct_2021/parquet/"
out_path = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/clusters-by-size/"

get_clusters_by_size(path, out_path, sizes = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])