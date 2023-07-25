# -*- coding: utf-8 -*-
"""
Created on Mon May 23 11:45:27 2022

@author: mathe
"""
import pandas as pd
import re
import pyarrow.parquet as pq
import os
from tqdm import tqdm
from openiti.helper.funcs import text_cleaner

pd.options.mode.chained_assignment = None

    
def tag_ms(all_cls, cluster_data_subset, clusters_for_text_df, no_clusters, ms_section, ms_int, safe_tags, tags = None):
    """The function that handles tagging the clusters into an individual milestone
    and returns the tagged milestone"""
    clusters = cluster_data_subset[cluster_data_subset["seq"] == ms_int].to_dict("records")
    # Only clean and tag out cluster if clusters exist                    
    if len(clusters) > 0:
        
        tagidxs_dict = []
        # If there are tags used to select the milestone -  For all tags log their index positions in the text, taking account for the cleaning operation
        if tags is not None:
            for each_tag in tags:
                tempsplits = re.split(each_tag, ms_section)
                
                for tidx, tempsplit in enumerate(tempsplits[:-1]):
                    indexpos = len(text_cleaner(" ".join(tempsplits[0:tidx+1])))
                    tagidxs_dict.append({"tag": each_tag, "index": indexpos, "tagged": False, "pos": 1000})
        
        # For all safe_tags log their index positions in the text, taking account for the cleaning operation
        
        # if len(safe_tags) > 1:    
        #     tag_regex = safe_tags[0]
        #     for safe_tag in safe_tags[1:]:
        #         tag_regex = tag_regex + "|" + safe_tag
        # else:
        #     tag_regex = safe_tags[:]
        splitter_tag = r"(" + safe_tags + ")"
        if len(re.findall(splitter_tag, ms_section)) > 0:
            tempsplits = re.split(splitter_tag, ms_section)
            for tidx, tempsplit in enumerate(tempsplits[:-1]):
                if not re.match(safe_tags, tempsplit):
                    indexpos = len(text_cleaner(" ".join(tempsplits[0:tidx+1])))
                    tagidxs_dict.append({"tag": tempsplits[tidx+1], "index": indexpos, "tagged": False, "pos": tidx})

                else:
                    continue
            
        # Convert the resulting dictionary into a df sort it by index (to facilitate mapping) and reconvert to dictionary    
        if len(tagidxs_dict) > 0:
            tagidxs_dict = pd.DataFrame(tagidxs_dict).sort_values(by = ["index", "pos"]).to_dict("records")    
        
        
        
        # Clean the milestone text ready for clusters mapping
        new_ms_text = text_cleaner(ms_section[:])
        
        # Create a mapping dictionary using the token offsets of the clusters - begin and end
        mapping_dict = []
        for cluster in clusters:
            
            mapping_dict.append({"cluster": cluster["cluster"], "type" : " @clb@" + str(cluster["size"]) + "@", "index" : cluster["begin"]})
            mapping_dict.append({"cluster": cluster["cluster"], "type" : " @cle@", "index" : cluster["end"]})
            
            # During the mapping process we look up every instance of the cluster in main dataset and add that to a dataframe
            cluster_df = all_cls[all_cls["cluster"] == cluster["cluster"]]
            cluster_df["tagged-text-ms"] = ms_int
            
            clusters_for_text_df = pd.concat([clusters_for_text_df, cluster_df])
            
        # Convert the resulting dictionary into a df sort it by index (to facilitate mapping) and reconvert to dictionary      
        mapping_dict = pd.DataFrame(mapping_dict).sort_values(by = ["index"]).to_dict("records")    
        
        # offset is the cumulitive count of character insertions into the text - everytime a tag is added to the text the offset is incremented by the length of the tag (this stops drift)
        offset = 0
        # We keep track of number of tags inserted - so when there are no tags remaining we stop looping through the tag dictionary
        tagged_count = 0
        total_tags = len(tagidxs_dict)
        # For each cluster mapping insert a tag        
        for mapping in mapping_dict:
            reusetag = mapping["type"] + str(mapping["cluster"]) + "@ "
            index = mapping["index"]
            if tagged_count != total_tags:
                for tagidx in tagidxs_dict:
                    # If there is a tag in the tag dictionary that occurs prior to the cluster tag and it has not yet been tagged - insert the tag
                    if tagidx["index"] < index and tagidx["tagged"] is False:
                        new_ms_text = new_ms_text[: tagidx["index"] + offset] + tagidx["tag"] + new_ms_text[tagidx["index"] + offset :]
                        offset = offset + len(tagidx["tag"])
                        tagidx["tagged"] = True                                               
                        tagged_count = tagged_count + 1
            # Once any tags have been handled - insert the cluster tag
            new_ms_text = new_ms_text[: mapping["index"] + offset] + reusetag + new_ms_text[mapping["index"] + offset :]
            offset = offset + len(reusetag)
        # If there are any remaining untagged tags after all clusters have been tagged out - make sure these are tagged too (this handles any tags that appear after all of the cluster tags)
        if tagged_count != total_tags:
            for tagidx in tagidxs_dict:
                if tagidx["tagged"] is False:
                    new_ms_text = new_ms_text[: tagidx["index"] + offset] + tagidx["tag"] + new_ms_text[tagidx["index"] + offset :]
                    tagidx["tagged"] = True
                    offset = offset + len(tagidx["tag"])
        return new_ms_text, clusters_for_text_df, no_clusters
    # If there are no clusters to tag - log the skipped milestone in no_clusters and return the existing milestone text and the log of clusters
    else:
        no_clusters.append(ms_int)
        return ms_section, clusters_for_text_df, no_clusters

def tag_clusters(tagged_texts, out_dest, cls_df, eval_cols=None, no_cl_csv = "no_cluster_list.csv", tags = None,
                        safe_tags = "###\s\|+\s|\n#\s|###\s\|+\s|PageV\d+P\d+|~~|\n|@YY\d{3}", add_tags = [], text_dir_type = 'folder', 
                        overwrite = False, write_only_tags = False):
    """
    tagged_texts can be passed to the function in two ways - the default text_dir_type 'folder' parses one folder containing a set of texts
    'file_list' takes a list of text files (given as absolute paths) and loops directly through them
    """
    # If a variable is passed in - take that as cl data
    
    all_cls = cls_df.copy()
    
    
    
    
    # Use metadata to filter according to requirements
    # Add book URI to make easier to read outputs
    # meta_df = pd.read_csv(meta_path, sep="\t")[["id", "date"]]
    # all_cls["id"] = all_cls["series"].str.split("-").str[0]
    # all_cls = pd.merge(all_cls, meta_df, how = "inner", on ="id")
    

    
    # If a folder is passed in (the default) loop through it and create a list of absolute paths
    if text_dir_type == "folder":
        file_list = []
        for root, dirs, files in os.walk(tagged_texts, topdown=False):
            for name in files:
                text_path = root + "/" + name
                file_list.append(text_path)
    
    # If a list of files is passed in - copy it into the file list to be used below
    if text_dir_type == "file_list":
        file_list = tagged_texts[:]
    
    
    # If overwrite is false - create a list of text ids from the out_dest
    if not overwrite:
        print(out_dest)
        exist_list = []
        for root, dirs, files in os.walk(out_dest, topdown=False):
            for name in files:
                book_id = ".".join(name.split(".")[0:-1])
                exist_list.append(book_id)
            print(exist_list)
    
    # Loop through the specified text_paths, collect cluster data for each text and insert tags
    for text_path in tqdm(file_list):
        if os.path.exists(text_path):
                # Get subset of cluster data relevant to text in question
                text_id = text_path.split("-ara")[0].split(".")[-1]
                name = ".".join(text_path.split("-ara")[0].split("/")[-1].split(".")[:-1])
                print(name)
                print(text_path)
                print(text_id)
                if not overwrite:
                    if name in exist_list:
                        print("Output file already exists... skipping...")
                        continue
                
                data_subset = all_cls[all_cls["id"] == text_id]
                clusters_for_text_df = pd.DataFrame()
                
                no_clusters = []
                
                
                
                text_cls_count = len(data_subset)
                print(text_cls_count)
                if text_cls_count == 0:
                    print("No clusters found")
                    continue
                with open(text_path, encoding = "utf-8") as f:
                    text = f.read()
                    f.close()
                # Split text into milestones and find those with tags - Need to split and retain a milestone ID, then
                # need to go through splits, find tagged ones, grap ms number and create a unique Id - URI+COUNT, OpenITI normalise,
                # find clusters and insert tags, output to csv, save text
                text = re.split(r"(ms\d+)", text)
                final_text = text[:]
                result_count = 0
                for idx, ms_section in enumerate(tqdm(text)):
                    # If we are only interacting with milestones with a reserve tag - do so
                    if tags is not None:
                        for tag in tags:
                            if re.search(tag, ms_section) is not None:
                                ms_int = int(text[idx+1].lstrip("ms"))
                                
                                # Get and tag data for ms
                                new_ms_text, clusters_for_text_df, no_clusters = tag_ms(all_cls, data_subset, clusters_for_text_df, no_clusters, ms_section, ms_int, safe_tags, tags = tags + add_tags)
                            
                                
                                # Replace the milestone with the tagged version in the final file
                                final_text[idx] = new_ms_text
                                
                               
                           
                            
                                
                            
                                # Repeat whole task for next ms
                                following_ms = ms_int + 1
                                ms_string = final_text[idx+2]
                                new_ms_text, clusters_for_text_df, no_clusters = tag_ms(all_cls, data_subset, clusters_for_text_df, no_clusters, ms_string, following_ms, safe_tags, tags = tags + add_tags)
                                final_text[idx+2] = new_ms_text  
                    # Otherwise tag the milestone and move on
                    else:
                        
                        if re.search(r"ms\d+", ms_section) is None:
                            try:
                                ms_int = int(text[idx+1].lstrip("ms"))                                
                                new_ms_text, clusters_for_text_df, no_clusters = tag_ms(all_cls, data_subset, clusters_for_text_df, no_clusters, ms_section, ms_int, safe_tags, tags = add_tags)
                                
                                final_text[idx] = new_ms_text
                            except IndexError:
                                
                                continue
                # To help avoid numbers being rounded - treat them as strings
                clusters_for_text_df["cluster"] = clusters_for_text_df["cluster"].astype(str)
                # Add any additional evaluation columns with a default value 'f'
                if eval_cols is not None:
                    for col in eval_cols:
                        clusters_for_text_df[col] = "f"
                
                # Reassemble the final text from the list
                final_text = "".join(final_text)
                # Create a df listing all ms without clusters
                no_clusters_df = pd.DataFrame(no_clusters, columns = ["milestone"])
                
                # Write out the transformed text into specified directory
                out_path_text = os.path.join(out_dest, name) + ".cl-tagged"
                print(out_path_text)
                with open(out_path_text, "w", encoding = "utf-8") as f:
                    f.write(final_text)
                    f.close()
                
                # Write out list of no cluster ms to specified location if not only cluster tags
                if not write_only_tags:
                    out_path_nocl = os.path.join(out_dest, name) + "_no-clusters.csv"
                    no_clusters_df.to_csv(out_path_nocl, index = False)
                
                
                    out_path_csv = os.path.join(out_dest, name + "cluster_csvs/")
                    if not os.path.exists(out_path_csv):
                        os.mkdir(out_path_csv)
                    
                    # Sort cluster df by the primary text milestone
                    clusters_for_text_df.sort_values(by = ["tagged-text-ms", "cluster"])
                    
                    # If clusters df is larger than 100 row, split it into multiple files - slice by clusters to avoid splitting of clusters between files. Label using ms of the primary file
                    total_rows = len(clusters_for_text_df)
                    clusters_for_text_df = clusters_for_text_df.reset_index(drop=True)                
                    if total_rows > 300:
                        current_index = 0
                        while current_index + 300 < total_rows:
                            
                            temp_slice = clusters_for_text_df.iloc[current_index: current_index + 300]
                                                    
                            last_cl = temp_slice.iloc[-1]["cluster"]
                            actual_end = clusters_for_text_df[clusters_for_text_df["cluster"] == last_cl]
                            actual_end = actual_end.index.values[-1]
                            
                            final_slice = clusters_for_text_df.iloc[current_index: actual_end+1]
                            first_ms = final_slice.iloc[0]["tagged-text-ms"]
                            last_ms = final_slice.iloc[-1]["tagged-text-ms"]
                            
                            final_slice.to_csv(out_path_csv + "/" + str(first_ms) + "-" + str(last_ms) + ".csv", encoding = 'utf-8-sig', index = False)
                            print("\nwritten: " + str(first_ms) + "-" + str(last_ms) + ".csv")
                            current_index = actual_end + 1
                        last_slice = clusters_for_text_df.iloc[current_index: -1]
                        if len(last_slice) > 0:
                            first_ms = last_slice.iloc[0]["tagged-text-ms"]
                            last_ms = last_slice.iloc[-1]["tagged-text-ms"]
                            last_slice.to_csv(out_path_csv + "/" + str(first_ms) + "-" + str(last_ms) + ".csv", encoding = 'utf-8-sig', index = False)
                    else:
                        first_ms = clusters_for_text_df.iloc[0]["tagged-text-ms"]
                        last_ms = clusters_for_text_df.iloc[-1]["tagged-text-ms"]
                        clusters_for_text_df.to_csv(out_path_csv + "/" + str(first_ms) + "-" + str(last_ms) + ".csv", encoding = 'utf-8-sig', index = False)

                        

