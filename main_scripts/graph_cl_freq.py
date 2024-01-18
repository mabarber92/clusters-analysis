import pandas as pd
import seaborn as sns
from tqdm import tqdm

def graph_cl_freq(cluster_csv, meta_csv, png_out, year_bound = 50):
    
    cluster_df = pd.read_csv(cluster_csv)
    meta_df = pd.read_csv(meta_csv, sep="\t")
    df_in = pd.merge(cluster_df, meta_df, how = "inner", on ="id")[["cluster", "date", "size", "book"]]
    df_in = df_in

    list_dict_out = []


    for i in tqdm(range(1, max(df_in["date"].to_list()), year_bound)):
        start = i
        end = i + year_bound - 1
        data_subset = df_in[df_in["date"].between(start, end)]
        total_clusters = len(data_subset)
        total_size = data_subset["size"].sum()
        total_books = len(data_subset["book"].drop_duplicates())
        list_dict_out.extend([{"start": start, "end": end, "count": total_clusters, "hue": "Total Clusters"},
        {"start": start, "end": end, "count": total_size, "hue": "Total Size"},
        {"start": start, "end": end, "count": total_books, "hue": "Total Books"}])

    data = pd.DataFrame(list_dict_out)
    g = sns.FacetGrid(data, hue="hue", sharex=True, sharey = False)
    g.map(sns.lineplot)
    # fig = g.get_figure()
    g.savefig(png_out)

if __name__ == "__main__":
    minfied_cl = "E:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500.csv"
    meta = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    out_path = "test_cl_freq.png"
    graph_cl_freq(minfied_cl, meta, out_path)
