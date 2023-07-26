# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 14:51:43 2021

@author: mathe
"""
import re
import pandas as pd
from tqdm import tqdm
import os
import pathlib
from statistics import median, mode

def pattern_map_dates(text, date_cats = [], add_terms = None, on = "head", date_summary = None, tops = None, w_counts = False, char_counts = False, map_terms = None):
    """ Takes date-range dictionary of {'beg': 0, 'end': 358, 'label': 'pre-Fatimid}
    date_summary can use one of two approaches to summarise one date from a potential set of 
     dates in a given section 'first' will take the first date found in the section - good for chronicles, 'median' - will take the median date - 'mode' the most
      common date in the section - if no date is found in the section the value will be 0. If no date summary is desired - set to none """
    out = []
    
    if on == "head":
        splits = re.split(r"###\s", text)
    if on == "ms":
        splits = re.split(r"ms\d+", text)

    if map_terms is not None:
        terms_copy = map_terms[:]
        for term in map_terms:
            count = len(re.findall(term, text))
            if count == 0:
                terms_copy.remove(term)
        
        
    word_char_counter = 0
 
 
    
    for idx, split in enumerate(tqdm(splits)):
        temp = {"section" : idx + 1}
        
        if w_counts:            
            sec_length = len(re.split(r"\s", split))           
        if char_counts:            
            sec_length = len(split)            
        
        temp["st_pos"]= word_char_counter
        word_char_counter = word_char_counter + sec_length       
        temp["mid_pos"] = word_char_counter + (sec_length/2)
        
        ### Iterate through date-ranges dict
        for item in date_cats:
            type_count = 0
            for i in range(item['beg'], item['end']):
                regex = r"@YY" + str(i).zfill(3)
                type_count = type_count + len(re.findall(regex, split))
            temp[item["label"]] = type_count

        # Find dates in section and tag either median or first
        if date_summary is not None:
            section_dates = re.findall(r"@YY(\d{3})", split)
            if len(section_dates) == 0:
                date = 0
            else:
                section_dates = [int(x) for x in section_dates]
                if date_summary == 'first':
                    date = int(section_dates[0])
                elif date_summary == 'median':
                    date = median(section_dates)
                elif date_summary == 'mode':
                    date = mode(section_dates)

            temp["date"] = date     

        if add_terms is not None:
            for term in add_terms:            
                count = len(re.findall(term, split))
                temp[term] = count
        
        if tops is not None:            
            topic = re.findall(tops, split)
            if len(topic) >= 1:
                temp["Topic_id"] = topic[0]
            else:
                temp["Topic_id"] = "None"
        
        if map_terms is not None:
            for term in terms_copy:            
                count = len(re.findall(term, split))
                temp[term] = count
        
        out.append(temp)
   
    out_df = pd.DataFrame(out)
    
    return out_df

def pattern_map_corpus(in_dir, out_dir, date_cats = [], add_terms = None, on = "head", date_summary = "first", tops = None, w_counts = False, char_counts = False, map_terms = None):
    for root, dirs, files in os.walk(in_dir, topdown=False):
        for name in files:
            text_path = os.path.join(root, name)
            out_path = os.path.join(out_dir, name + "-date-mapped.csv")
            with open(text_path, encoding='utf-8-sig') as f:
                text = f.read()
            
            mapDf = pattern_map_dates(text, date_cats = date_cats, add_terms = add_terms, on = on, date_summary = date_summary, tops = tops, w_counts = w_counts, char_counts = char_counts, map_terms = map_terms)
            mapDf.to_csv(out_path, encoding='utf-8-sig')
            


# in_path = "C:/Users/mathe/Documents/Github-repos/fitna-study/whole text tagger/outputs/dates_tagged_new"
# out_path = "C:/Users/mathe/Documents/Github-repos/fitna-study/dates_analysis/mappings_mamluk_split"


# cats = [{'beg': 1, 'end': 100, 'label': 'first-century'}, 
#         {'beg': 101, 'end': 357, 'label': 'pre-fatimid'}, 
#         {'beg': 358, 'end': 567, 'label': 'fatimid'},
#         {'beg': 568, 'end': 648, 'label': 'ayyubid'},
#         {'beg': 649, 'end': 783, 'label': 'bahri-mamluk'},
#         {'beg': 784, 'end': 900, 'label': 'circassian-mamluk'}]


# for root, dirs, files in os.walk(in_path, topdown=False):
#     for name in tqdm(files):
#         print(name)            
#         text_path = os.path.abspath(os.path.join(root, name))
#         section_path = out_path + "/sections/"
#         ms_path = out_path + "/ms/"
#         if not os.path.exists(section_path):
#             pathlib.Path(section_path).mkdir(parents=True, exist_ok=True)
#         if not os.path.exists(ms_path):
#             pathlib.Path(ms_path).mkdir(parents=True, exist_ok=True)
#         section_path = out_path + "/sections/" + name + ".s_mapped.csv"
#         ms_path = out_path + "/ms/" + name + ".ms_mapped.csv"
        
#         with open(text_path, encoding = "utf-8") as f:
#             text = f.read()
#             f.close()
            
#         section_mappings = pattern_map_cat(text, date_cats = cats, tops = False, w_counts=True, char_counts = False)
#         section_mappings.to_csv(section_path)
        
#         ms_mappings = pattern_map_cat(text, date_cats = cats, on="ms", tops = False, w_counts=True, char_counts = False)
#         ms_mappings.to_csv(ms_path)
        
    