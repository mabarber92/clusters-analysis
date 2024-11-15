# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 17:27:02 2022

@author: mathe
"""

import matplotlib.pyplot as plt

from matplotlib import patches
import pandas as pd

def plot_reuse(reuse_map, out, maintext, section_map = None, top_colours = None, annotation = None, label_conv = None, add_dates=False, overlap_patch = True, 
        b2_focus_graph = None, concat_width = 500):
    """b2_focus_graph supplies a URI for the main b2 and concatenates the remaining books into bars with reuse frequency, according to the concat_width"""
    reuse_df = pd.read_csv(reuse_map, encoding="utf")
    
    if label_conv is not None:
        reuse_df_dict = reuse_df.to_dict("records")
        for row in reuse_df_dict:
            if row["Text"] in label_conv.keys():          
                new_label = label_conv[row["Text"]]
                row["Text"] = new_label
            
        reuse_df = pd.DataFrame(reuse_df_dict)

    
    fig, axs = plt.subplots(1, 1)
    fig.set_size_inches(10, 6)
    
    if section_map is not None:
        section_df = pd.read_csv(section_map)
        if len(section_df) > 0:
            if top_colours is not None:
                box = axs.get_position()
                axs.set_position([box.x0, box.y0 + box.height * 0.1,
                    box.width, box.height * 0.9])
                for top in top_colours:
                    top_sections = section_df[section_df["Topic_id"] == top["id"]]
                    axs.vlines("st_pos", ymin = -1, ymax = -0.1, colors= top["colour"], data=top_sections, linewidth = 1, label = top["label"])
                axs.legend(loc='upper center', title = "Dynastic period described in section", ncol=len(top_colours), bbox_to_anchor=(0.4, -0.12))
            else:            
                axs.vlines("st_pos", ymin = -1, ymax = -0.1, colors= 'black', data=section_df, linewidth = 1, label = "Section\nboundary")

            y_value_list = [-0.5]
            plt.annotate("start", (section_df["st_pos"].to_list()[0] , -1.2), ha="center", va="center", size=6)
            plt.annotate("end", (section_df["st_pos"].to_list()[-1] , -1.2), ha="center", va="center", size=6)
        else:
            section_map = None
            y_value_list = []
        
    else:
        y_value_list = []
    
    if add_dates:
        x_value_list = section_df["st_pos"].to_list()
        x_label_list = section_df["date"].to_list()
    
    text_list = reuse_df["Text"].sort_values().drop_duplicates().to_list()

    if "Other" in text_list:
        text_list.remove("Other")
    if overlap_patch:
        data_dict = reuse_df.to_dict("records")
    else:
        data_dict = reuse_df[reuse_df["Text"] == "Other"].to_dict("records")
    
    if not b2_focus_graph:
        for reuse_instance in data_dict:
            sec_width = (reuse_instance["ch_end_tar"] - reuse_instance["ch_start_tar"])
            patch = patches.Rectangle(xy = (reuse_instance["ch_start_tar"], 0), width = sec_width, height = 1 * len(text_list), color = "lightgrey")
            axs.add_patch(patch)

    plt.savefig("test1.png", dpi=300, bbox_inches = "tight")

    if b2_focus_graph:
        non_b2_lengths = []
        reuse_dict_list = reuse_df.sort_values(by=["ch_start_tar"]).to_dict("records")
        last_end_pos = 0
        y_value_list.append(0.5)
        for idx, reuse_instance in enumerate(reuse_dict_list):
            if reuse_instance["Text"] == b2_focus_graph:
                
                sec_width = (reuse_instance["ch_end_tar"] - reuse_instance["ch_start_tar"])
                patch = patches.Rectangle(xy = (reuse_instance["ch_start_tar"], 0), width = sec_width, height = 0.9, color = "black")      
                axs.add_patch(patch)
                
                # This plus else only needed for more detailed bars between b2 instances
                last_end_pos = reuse_instance["ch_end_tar"]
            else:
                for reuse_instance in reuse_dict_list[idx+1:]:
                    if reuse_instance["Text"] == b2_focus_graph:
                        gap_end = reuse_instance["ch_start_tar"]
                non_b2_lengths.append({
                    "start": last_end_pos,
                    "end": gap_end
                })
        
        book_length = reuse_df["ch_end_tar"].max()
        
        slice_count = int(book_length/concat_width)
        max_text_count = 0
        
        for start in range(0, book_length, concat_width,):
            end = start + concat_width + 1
            
            filtered_df = reuse_df[reuse_df["ch_start_tar"] >= start]
            filtered_df = filtered_df[filtered_df["ch_start_tar"] < end]
            text_count = len(filtered_df["Text"].drop_duplicates())
            if text_count > max_text_count:
                max_text_count = text_count
            patch = patches.Rectangle(xy = (start, 1), width = concat_width, height = text_count, color = "black")      
            axs.add_patch(patch)
        
        count_labels = []
        for i in range(1, max_text_count):
            y_value_list.append(i)
            count_labels.append(str(i - 1))

        # # Loop through non_b2_lengths and concatenate slices
        ### This code perhaps over-detailed - focussing on only drawing graphs in the gap
        # for non_b2_length in non_b2_lengths:
        #     length = non_b2_length["end"] - non_b2_length["start"]
        #     slice_count = length/concat_width
        #     for start in range(non_b2_length["start"], slice_count, concat_width):
        #         end = start + concat_width
        #         filtered_df = reuse_df[reuse_df["ch_start_tar"] => start]
        #         filtered_df = filtered_df[filtered_df["ch_end_tar"] < end]
        #         text_count = len(filtered_df["Text"].drop_duplicates())
        #         patch = patches.Rectangle(xy = (start, 1), width = concat_width, height = text_count, color = "black")      
        #         axs.add_patch(patch)
                



    else:
        for idx, text in enumerate(text_list):
            data_dict = reuse_df[reuse_df["Text"] == text].to_dict("records")
            y_value_list.append(idx + 0.5)
            for reuse_instance in data_dict:
                sec_width = (reuse_instance["ch_end_tar"] - reuse_instance["ch_start_tar"])
                patch = patches.Rectangle(xy = (reuse_instance["ch_start_tar"], idx), width = sec_width, height = 0.9, color = "black")
                axs.add_patch(patch)
    
    plt.savefig("test2.png", dpi=300, bbox_inches = "tight")
    
    if b2_focus_graph and section_map is not None:
        label_list = ["Section Boundary", b2_focus_graph] + count_labels
        
    elif section_map is not None:
        label_list = ["Section Boundary"] + text_list
    else:
        label_list = text_list[:]
        
    
    if annotation is not None:
        for annot in annotation:
            plt.annotate(annot["text"], (annot["x"], annot["y"]))
    
    
   
    plt.yticks(y_value_list, label_list)

    if add_dates:
        plt.xticks(x_value_list, x_label_list)
        plt.xticks(rotation=90)
    # else:
    #     x_ticks = list(range(0, 4000000, int(text_char_length/6)))
    #     print(x_ticks)
    #     plt.xticks(x_ticks, x_ticks)

    plt.xlabel("Number of characters into the " + maintext)
    
    fig.savefig(out, dpi=300, bbox_inches = "tight")
    
    plt.show

if __name__ == "__main__":

    reuse_map = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/map_clusters_to_text/data_out/reuse_maps/0845Maqrizi.ItticazHunafa.cl-tagged-reuse.csv"
    section_map = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/map_clusters_to_text/data_out/reuse_maps/0845Maqrizi.ItticazHunafa.cl-tagged-section.csv"
    graph_dir = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/map_clusters_to_text/test_freq_bar.jpeg"

    # topics = [{"id": "@PREIS@", "colour": "brown", "label" : "Pre-Islamic"},
    #       {"id": "@EARIS@", "colour": "yellow", "label" : "Early Islamic"},
    #       {"id": "@IKH@", "colour": "orange", "label" : "Iḫšīdid"},
    #       {"id": "@FAT@", "colour": "green", "label" : "Fāṭimid"},
    #       {"id": "@AYY@", "colour": "red", "label" : "Ayyūbid"},
    #       {"id": "@MAM@", "colour": "darkblue", "label" : "Mamlūk"},
    #       {"id": "None", "colour": "black", "label" : "No Dynasty"}
    #       ]



    # labels ={"0845Maqrizi.ItticazHunafa": "Ittiʿāẓ al-Ḥunafāʾ",
    #      "0845Maqrizi.Mawaciz": "Ḫiṭaṭ",
    #      "0845Maqrizi.Rasail": "Rasāʾil",
    #      "0845Maqrizi.ShudhurCuqud": "Šuḏūr al-ʿUqūd",
    #      "0845Maqrizi.NuqudQadima" : "al-Nuqūd al-Qadīma",
    #      "0845Maqrizi.Muqaffa": "al-Muqaffā al-Kabīr"}

    plot_reuse(reuse_map, graph_dir, maintext='Ittiaz', add_dates = True, section_map=section_map, b2_focus_graph="0677IbnMuyassar.AkhbarMisr" )
