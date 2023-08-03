import sys
import os
sys.path.append(os.path.abspath('../'))
print(sys.path)
from main_scripts.clusterDf import clusterDf
from main_scripts.cls_text_tag import tag_clusters

import pandas as pd
import re


def return_corpus_paths_for_books(meta_path, openiti_corpus_base, book_list):
    meta_df = pd.read_csv(meta_path, sep="\t")
    meta_df[meta_df["status"] == "pri"]
    meta_df["rel_path"] = openiti_corpus_base + meta_df["local_path"].str.split("/master/|\.\./", expand = True, regex=True)[1]    
    path_list = meta_df[meta_df["book"].isin(book_list)]["rel_path"].to_list()
    return path_list

def merge_split_with_splitter(splits, splitter_regex, before=True):
    """Loop through a set of splits and merge each split with its deliniator - before means that the splitter should be attached before the corresponding split, if set to false the splitter will
    be attached to the text in the split that precedes it"""
    new_splits = []
    total_indices = len(splits) - 1
    print(splitter_regex)
    for idx, split in enumerate(splits):        
        if re.match(splitter_regex, split):
            if before:
                if idx == total_indices:
                    new_text = split
                else:                     
                    new_text = "".join([splits[idx], splits[idx+1]])
            else:
                new_text = "".join([splits[idx-1], splits[idx]]) 
            new_splits.append(new_text)
    return new_splits


def create_cluster_splitter_csvs(tagged_text_path, text_id, author_uri, out_path, splitter="(###\s\|\s)"):
    """By default we do the splitting on the level one headings - this task would become more difficult for other levels"""
    with open(tagged_text_path, encoding='utf-8') as f:
        text = f.read()

    splits = re.split(splitter, text)
    splits = merge_split_with_splitter(splits, splitter)

    cluster_section_ids_dicts = []
    meta_template_dicts = []

    for idx, split in enumerate(splits):
        found_clusters = re.findall("@clb@\d+@(\d+)@", split)
        if len(found_clusters) != 0:
            section_titles = re.findall(r"###\s\|+[^\n\r]+", split)
            if len(section_titles) > 0:
                section_title = section_titles[0]
            else:
                section_title = "No title found"            

            section_id = text_id + "Sec" + str(idx)
            
            for cluster in found_clusters:
                cluster_section_ids_dicts.append({"cluster": int(cluster), "id": section_id})
            meta_template_dicts.append({"id": section_id, "section_title": section_title, "book": author_uri + "."})
    
    meta_template_df = pd.DataFrame(meta_template_dicts)
    cluster_section_ids_df = pd.DataFrame(cluster_section_ids_dicts)
    file_name = tagged_text_path.split("-")[0].split("/")[-1]
    clusters_out = os.path.join(out_path, file_name + "cluster-section-ids.csv")
    meta_out = os.path.join(out_path, file_name + "section-ids-meta.csv")
    
    cluster_section_ids_df.to_csv(clusters_out, encoding='utf-8-sig', index = False)
    meta_template_df.to_csv(meta_out, encoding='utf-8-sig', index = False)




def create_cluster_section_mappings(cluster_path, meta_path, book_uri, openiti_corpus_base, out_dir):
    
    # Get the path of the book
    book_path = return_corpus_paths_for_books(meta_path, openiti_corpus_base, [book_uri])

    # Load a cluster object
    cluster_df = clusterDf(cluster_path, meta_path).cluster_df
    
    # Tag the clusters into the text
    tagged_cluster_path = os.path.join(out_dir, book_uri + ".cl-tagged")
    tag_clusters(book_path, out_dir, cluster_df, write_only_tags=True, text_dir_type="file_list")

    #Open the tagged text - apply function to map headers to clusters
    text_id = book_path[0].split("-ara")[0].split(".")[-1]
    author_id = book_uri.split(".")[0]
    create_cluster_splitter_csvs(tagged_cluster_path, text_id, author_id, out_dir)

if __name__ == "__main__":
    cluster_path = "D:/Corpus Stats/2023/v7-clusters/minified_clusters_pre-1000AH_under500.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2022-2-7_merged.csv"
    book_uri = "0845Maqrizi.Rasail"
    openiti_corpus_base = "D:/OpenITI Corpus/corpus_2022_2_7/"
    out_dir = "maqrizi.rasail_sections/"
    create_cluster_section_mappings(cluster_path, meta_path, book_uri, openiti_corpus_base, out_dir)