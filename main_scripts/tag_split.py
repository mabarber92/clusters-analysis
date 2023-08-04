# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 09:39:20 2022

@author: mathe
"""

import pandas as pd
import re
from openiti.helper.funcs import text_cleaner



def tag_split(all_cls, cluster_data_subset, clusters_for_text_df, no_clusters, ms_section, ms_int, safe_tags, tags = None):
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