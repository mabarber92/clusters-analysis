# from clusterDf import clusterDf
import pandas as pd
import re
from openiti.helper.funcs import text_cleaner
## TO DO - CLEAN THE TEXT BEFORE PASSING IT TO LEN WHEN PASSING OVER SPLITS - OR WE'RE GOING TO GET MAPPING ISSUES

def get_tagged_spans(text_path, start_tag, end_tag):
    """Take text path and defined start and end tags extract positions and output as a df, formatted as
    'ms', 'start_pos', 'end_pos' 
   In the present setup the tags will be excluded from the final offsets - ready for passim mapping
   start_tag and end_tag must be regex compliant"""
    ## Read in text
    with open(text_path, encoding='utf-8') as f:
        text = f.read()
    
    ## Split the text into ms
    ms_splits = re.split(r"(ms\d+)", text)
    print(len(ms_splits))
    ## Define output list
    out = []
    
    ## Create splitter regex from tags
    splitter_regex = r"({}|{})".format(start_tag, end_tag)

    ## Loop through ms_splits and build output
    for idx, ms in enumerate(ms_splits):
        if re.search(r"ms\d+", ms) is None:
            try:
                ms_int = int(ms_splits[idx+1].lstrip("ms"))
            except IndexError:
                continue
            
            
        
        tag_splits = re.split(splitter_regex, ms)
        cumulative_count = 0
        
        for split_idx, split in enumerate(tag_splits):
            
            if re.search(start_tag, split) is not None:               
               start_pos = cumulative_count
               string = tag_splits[split_idx+1]
               end_pos = cumulative_count + len(text_cleaner(string))
               out.append({"ms": ms_int, "string": string, "start_pos": start_pos, "end_pos": end_pos})
            elif re.search(end_tag, split) is not None:
                continue
            else:
                cumulative_count = cumulative_count + len(text_cleaner(split))
    
    ## Transform out into df
    out_df = pd.DataFrame(out)

    ## Return df
    return out_df


if __name__ == "__main__":
    text_path = "D:/OpenITI Corpus/corpus_2022_2_7/data/0310Tabari/0310Tabari.JamicBayan/0310Tabari.JamicBayan.Shamela0007798-ara1.inProgress"

    df = get_tagged_spans(text_path, "\{", "\}")
    df.to_csv("extraction_text", index=False, encoding='utf-8-sig')




