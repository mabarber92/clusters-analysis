# -*- coding: utf-8 -*-
"""
This is a specific function to deal with alterations to the Ighatha after the passim run. As
the biggest change was the removal of footnotes, offsets at a page level are less likely to be
off than milestone offsets. - this performs very well in this context. Only a few character slips - sufficient for larger viz
"""

from clusterDf import clusterDf
from tag_split import tag_split
import re
from tqdm import tqdm
import pandas as pd
from openiti.helper.funcs import text_cleaner

def tag_page(page_text, page_map, safe_tags):
    tagidxs_dict = []
    splitter_tag = r"(" + safe_tags + ")"
    if len(re.findall(splitter_tag, page_text)) > 0:
        tempsplits = re.split(splitter_tag, page_text)
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
    new_ms_text = text_cleaner(page_text[:])
    
    
        
    # Convert the resulting dictionary into a df sort it by index (to facilitate mapping) and reconvert to dictionary      
    mapping_dict = pd.DataFrame(page_map).sort_values(by = ["index"]).to_dict("records")    
    
    # offset is the cumulitive count of character insertions into the text - everytime a tag is added to the text the offset is incremented by the length of the tag (this stops drift)
    offset = 0
    # We keep track of number of tags inserted - so when there are no tags remaining we stop looping through the tag dictionary
    tagged_count = 0
    total_tags = len(tagidxs_dict)
    # For each cluster mapping insert a tag        
    for mapping in mapping_dict:
        reusetag = mapping["type"]
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
    return new_ms_text

def map_page_offset(all_cls, text_path, new_text_path, book_uri,  safe_tags = "###\s\|+\s|\n#\s|###\s\|+\s|PageV\d+P\d+|~~|\n"):
    "Splits text on ms, maps the clusters into the text, resplits on pages and calculates offsets"
    with open(text_path, encoding='utf-8') as f:
        text = f.read()
    text = re.split(r"(ms\d+)", text)
    final_text = text[:]
    no_clusters = []
    add_tags = []
    
    data_subset = all_cls[all_cls["book"] == book_uri]
    clusters_for_text_df = pd.DataFrame()
    
    print("starting tagging")
    
    for idx, ms_section in enumerate(tqdm(text)):
        # If we are only interacting with milestones with a reserve tag - do so
        
        if re.search(r"ms\d+", ms_section) is None:
            try:
                ms_int = int(text[idx+1].lstrip("ms"))                                
                new_ms_text, clusters_for_text_df, no_clusters = tag_split(all_cls, data_subset, clusters_for_text_df, no_clusters, ms_section, ms_int, safe_tags, tags = add_tags)
                
                final_text[idx] = new_ms_text
            except IndexError:
                
                continue

    
    # Reassemble the final text from the list
    final_text = "".join(final_text)
    
    # Create dict - to be populated as follows : {PageCode: {type: clusterTag, index: charPos}}
    page_map = {}
    print(len(re.findall(r"@clb@\d+@\d+@", final_text)))
    # Split text on page numbers and populate dict
    page_split = re.split(r"(PageV\d+P\d+)", final_text)
    total_splits = len(page_split)
    for idx, pg_section in enumerate(tqdm(page_split)):
        
        
        if re.search(r"PageV\d+P\d+", pg_section) is None and idx != total_splits-1:
            pg_code = page_split[idx+1]
            
            
            # Split section on beg positions
            if len(re.findall(r"(@cl[eb]@?\d*@\d+@)", pg_section)) > 0:
                
                beg_splits = re.split(r"(@cl[eb]@?\d*@\d+@)", pg_section)
                total = len(beg_splits)
                for b_idx, split in enumerate(beg_splits):
                    
                    if re.search(r"(@cl[eb]@?\d*@\d+@)", split) is None and b_idx != total-1:
                        
                        cluster = beg_splits[b_idx+1]
                        
                        if pg_code not in page_map.keys():
                            page_map[pg_code] = []
                        preceeding_text_len = len(text_cleaner(" ".join(beg_splits[0:b_idx+1])))
                        page_map[pg_code].append({"type": cluster, "index" : preceeding_text_len})
                        
    """Below commented out code creates different kind of page dictionary - might be useful for
    other types of tagging
    # # Split text on page numbers and populate dict
    # page_split = re.split(r"(PageV\d+P\d+)", final_text)
    
    # for idx, pg_section in enumerate(tqdm(page_split)):
        
        
    #     if re.search(r"PageV\d+P\d+", pg_section) is None:
    #         pg_code = page_split[idx-1]
            
            
    #         # Split section on beg positions
    #         if len(re.findall(r"(@clb@\d+@\d+@)", pg_section)) > 0:
                
    #             beg_splits = re.split(r"(@clb@\d+@\d+@)", pg_section)
    #             total = len(beg_splits)
    #             for b_idx, split in enumerate(beg_splits):
                    
    #                 if re.search(r"@clb@\d+@\d+@", split) is None and b_idx != total-1:
                        
    #                     cluster = beg_splits[b_idx+1].split("@")[-2]
                        
    #                     if cluster not in page_map.keys():
    #                         page_map[cluster] = {}
    #                     preceeding_text_len = len(text_cleaner(" ".join(beg_splits[0:b_idx+1])))
    #                     page_map[cluster]["page_beg"] = pg_code
    #                     page_map[cluster]["beg"] = preceeding_text_len
            
    #         if len(re.findall(r"(@cle@\d+@)", pg_section)) > 0:
    #             end_splits = re.split(r"(@cle@\d+@)", pg_section)                
    #             total = len(end_splits)
    #             for e_idx, split in enumerate(end_splits):
    #                 if re.search(r"@cle@\d+@", split) is None and e_idx != total-1:
                        
    #                     cluster = end_splits[e_idx+1].split("@")[-2]
                       
    #                     if cluster not in page_map.keys():
    #                         page_map[cluster] = {}
    #                     preceeding_text_len = len(text_cleaner(" ".join(end_splits[0:e_idx+1])))
    #                     page_map[cluster]["page_end"] = pg_code
    #                     page_map[cluster]["end"] = preceeding_text_len
    """
    
    # Split new text on pages, pass each page split with the map and safe tags to the tagger
    with open(new_text_path, encoding='utf-8') as f:
        new_text = f.read()
    new_text = re.split(r"(PageV\d+P\d+)",new_text)
    new_text_out = new_text[:]
    for idx, page in enumerate(tqdm(new_text)):
        # If we are only interacting with milestones with a reserve tag - do so
        
        if re.search(r"PageV\d+P\d+", ms_section) is None:
            try:
                page_id = new_text[idx+1]
                if page_id in page_map.keys():                              
                    tagged_text = tag_page(page, page_map[page_id], safe_tags)
                else:
                    continue
                new_text_out[idx] = tagged_text
            except IndexError:
                
                continue
    
    new_text_out = "".join(new_text_out)

    # Overwrite the input text
    with open(new_text_path, "w", encoding = "utf-8-sig") as f:
        f.write(new_text_out)
        f.close()

if __name__  == "__main__":
    text_path = "D:/OpenITI Corpus/corpus_2022_2_7/data/0845Maqrizi/0845Maqrizi.IghathaUmma/0845Maqrizi.IghathaUmma.Kraken210223142017-ara1"
    new_text_path = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/map_clusters_to_text/data_out_igatha_corrected/clusters_tagged/0845Maqrizi.IghathaUmma.Kraken210223142017-ara1.dyn-tagged"
    cluster_path = "D:/Corpus Stats/2023/v7-clusters/minified_clusters_pre-1000AH_under500.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2022-2-7.csv"
    book_uri = "0845Maqrizi.IghathaUmma"

    cluster_df = clusterDf(cluster_path, meta_path).cluster_df

    map_page_offset(cluster_df, text_path, new_text_path, book_uri)

