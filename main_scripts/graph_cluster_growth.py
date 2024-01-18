import pandas as pd
import seaborn as sns
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial

def map_scatter(cluster_id, cluster_df, counter_start = 0):
    cluster_filter = sorted(cluster_df[cluster_df["cluster"] == cluster_id]["date"].to_list())
    df_dict_list_out = []

    if not len(cluster_filter) < 3:            
        for idx, cluster_item in enumerate(cluster_filter):
            df_dict_list_out.append({"cluster" : cluster_id, "date" : cluster_item, "size" : idx + 1})
    return df_dict_list_out

def graph_cluster_growth(cluster_csv, meta_csv, png_out, csv_out):
    cluster_df = pd.read_csv(cluster_csv)

    # Drop clusters that are 2 or under
    cluster_df = cluster_df[cluster_df["size"] > 2]
    meta_df = pd.read_csv(meta_csv, sep="\t")
    df_in = pd.merge(cluster_df, meta_df, how = "inner", on ="id")[["cluster", "date"]]
    df_in = df_in

    cluster_list = df_in["cluster"].drop_duplicates().to_list()
    

    # Create input
    print("creating data")
    with Pool(8) as p:
        list_of_list_out = list(tqdm(map(partial(map_scatter, cluster_df=df_in), cluster_list), total=len(cluster_list)))
        

    df_dict_list_out = [item for sublist in list_of_list_out for item in sublist]
    out_df = pd.DataFrame(df_dict_list_out)
    out_df.to_csv(csv_out)    


    print("Plotting...")
    # Plot scatter
    g = sns.scatterplot(data = out_df, x="date", y = "size", alpha=0.2)
    fig = g.get_figure()
    fig.savefig(png_out)


if __name__ == "__main__":
    minfied_cl = "E:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500.csv"
    meta = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    out_path = "test_cl_growth_scatter.png"
    csv_out = "test_cl_growth.csv"
    graph_cluster_growth(minfied_cl, meta, out_path, csv_out)