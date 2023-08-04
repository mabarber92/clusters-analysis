import os
import sys
import pandas as pd
#Import main dependencies
from taggers.tag_years_whole_text import tag_whole_text_dir
sys.path.append(os.path.abspath('../'))
from graph.graph_reuse import plot_reuse
from reuse.multi_reuse_map import multi_reuse_map_corpus


#Import broader cluster dependencies
sys.path.append(os.path.abspath('../..'))
from main_scripts.clusterDf import clusterDf
from main_scripts.cls_text_tag import tag_clusters
from create_md_uris.update_cl_vers_ids import update_cls_vers_ids

def return_corpus_paths_for_books(meta_path, openiti_corpus_base, book_list):
    meta_df = pd.read_csv(meta_path, sep="\t")
    meta_df["rel_path"] = openiti_corpus_base + meta_df["local_path"].str.split("/master/|\.\./", expand = True, regex=True)[1]    
    path_list = meta_df[meta_df["book"].isin(book_list)]["rel_path"].to_list()
    return path_list

def graph_for_dated_sections(in_books, out_dir, corpus_base_path, meta_path, cluster_dir, existing_cluster_tagged = [], pairs_focus=None, max_reuse_date=None, min_reuse_date=0, 
                             cluster_cap = 100, date_section_range = [], date_cats=[], date_summary='first', tops=None, new_ids_paths =[]):
    """in_books gives a list of book_uris to be used as the main texts for the graphs - a graph and intermediary files 
    will be produced for each
    pairs_focus gives a list of book_uris - only these uris will be used for the analysis - clusters that
    do not contain those URIs will be excluded from the analysis. None means all pairs will be considered
    cluster_cap sets the maximum cluster size to be graphed - this stops really large clusters creating a very
    tall y-axis if this is not desired
    date_section_range gives a range of dates for which to filter sections - only really applies for chronicles -
    [360,500] would give sections between years 360 and 500
    tops - one dictionary that tells the mapper which tags to extract and the id to give, in addition to the 
    colour given in the graph for the tag - TO DO - Add an example of an input dictionary for this field
    TO DO - UNIFY HOW WE'RE DEALING WITH DYNASTIC MAPPINGS AND COLOURS -
      FOR THIS EACH SECTION NEEDS A DYNASTIC MAPPING -
    WILL NEED TO ADD ANOTHER FUNCTION
    new_ids_paths is a list of dictionaries giving fields "new_ids" "new_ids_meta", where both supply absolute
     paths to the csv files for the changes. Example:
     [{"new_ids": "/0845MaqriziRasaili.cluster-section-ids.csv",
        "new_ids_meta": "//0845MaqriziRasaili.section-ids-meta.csv"
        }] 
    If it is given then the version ids for the clusters will be renamed using the data supplied. If it is left empty
    then the clusters will be left as normal

    For tops use a list of dictionaries like this:
        topics = [{"id": "@PREIS@", "colour": "brown", "label" : "Pre-Islamic"},
          {"id": "@EARIS@", "colour": "yellow", "label" : "Early Islamic"},
          {"id": "@IKH@", "colour": "orange", "label" : "Ikhshidid"},
          {"id": "@FAT@", "colour": "green", "label" : "Fatimid"},
          {"id": "@AYY@", "colour": "red", "label" : "Ayyubid"},
          {"id": "@MAM@", "colour": "darkblue", "label" : "Mamluk"},
          {"id": "None", "colour": "black", "label" : "No Dynasty"}
          ]

    """
    
    # If date_section_range is set then date_summary must be used - use a default of 'first'
    if len(date_section_range) == 2 and date_summary is None:
        date_summary = 'first'

    # Create main out_dir if it doesn't exist
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    # Load metadata - get the paths for the main input texts - if existing_cluster_tagged paths have not been supplied
    if len(existing_cluster_tagged) > 0:
        in_paths = existing_cluster_tagged[:]
        use_cluster_tagger = False
    else:
        in_paths = return_corpus_paths_for_books(meta_path, corpus_base_path, in_books)
        print(in_paths)
        if len(in_paths) == 0:
            print("Supplied book URIs not found: {} \n try again".format(in_books))
            exit()
        use_cluster_tagger = True
    
    print("Loading clusters")
    # Load in clusters as a clusterDf Obj
    clsObj = clusterDf(cluster_dir, meta_path, min_date=min_reuse_date, max_date=max_reuse_date, cluster_cap=cluster_cap)
    
    # If there is a pairs focus - only keep those in the pairs focus + plus input texts
    if pairs_focus is not None:
        pairs_focus = pairs_focus.extend(in_books)
        clsObj.filter_by_book_list(pairs_focus)
    
    cluster_df = clsObj.cluster_df

    if len(new_ids_paths) > 0:
        cluster_df = update_cls_vers_ids(cluster_df, new_ids_paths)

    maps_out_dir = os.path.join(out_dir, "reuse_maps")
    if not os.path.exists(maps_out_dir):
        os.mkdir(maps_out_dir)

    # Folder for the clusters tagged into the main text
    tagged_cluster_dir = os.path.join(out_dir, "clusters_tagged")
    if not os.path.exists(tagged_cluster_dir):
        os.mkdir(tagged_cluster_dir)

    # Add date tags to the texts if date_summary is being used
    if date_summary is not None:
        print("Tagging dates")
        input_dir = os.path.join(out_dir, "clusters_dates_tagged")
        if not os.path.exists(input_dir):
            os.mkdir(input_dir)
        
        
        
        tag_whole_text_dir(in_paths, input_dir, path_list=True)
        add_dates = True

        # Then tag clusters
        print("Tagging clusters")
        if use_cluster_tagger:
            tag_clusters(input_dir, tagged_cluster_dir, cluster_df, text_dir_type = 'folder', overwrite = True, write_only_tags=True)
        
    else:
        print("Skipping dates tagging..")
        print("Tagging clusters")
        add_dates = False
        if use_cluster_tagger:
            tag_clusters(in_paths, tagged_cluster_dir, cluster_df, text_dir_type = 'file_list', overwrite = True, write_only_tags=True)
    
    
    

    
    if tops is not None:
        topic_tags = []
        for top_dict in tops:
            if top_dict["id"] != "None":
                topic_tags.append(top_dict["id"])
        print(topic_tags)
    
    else:
        topic_tags = None

    
    print("Creating reuse map")
    multi_reuse_map_corpus(cluster_df, tagged_cluster_dir, maps_out_dir, section_map = True, date_summary=date_summary, date_cats = date_cats, tops=topic_tags)

    #If we have a date filter - make a new folder containing filtered map - change the input dir
    print(date_section_range)
    if len(date_section_range) == 2:
        print("Filtering to date range")
        date_filtered_path = os.path.join(out_dir, "dates_range_filtered")
        if not os.path.exists(date_filtered_path):
            os.mkdir(date_filtered_path)
        for root, dirs, files in os.walk(maps_out_dir):
            for name in files:
                input_parts = name.split("-")
                text_name = ".".join(name.split(".")[:2])
                if input_parts[-1] == 'section.csv':
                    csv_in = os.path.join(root, name)
                    df = pd.read_csv(csv_in)
                    df_sections = df[df["date"].between(date_section_range[0], date_section_range[1])]
        
        data_path = os.path.join(root, "-".join(input_parts[:-1]) + "-reuse.csv")
        df_data = pd.read_csv(data_path)
        max_extent = df_sections["mid_pos"].max()
        min_extent = df_sections["st_pos"].min()
        df_data = df_data[df_data["ch_start_tar"] >= min_extent]
        df_data = df_data[df_data["ch_end_tar"] <= max_extent]
        
        data_csv_path = os.path.join(date_filtered_path, '{}-{}-{}-reuse.csv'.format(name, date_section_range[0], date_section_range[1]))
        sections_csv_path = os.path.join(date_filtered_path, '{}-{}-{}-section.csv'.format(name, date_section_range[0], date_section_range[1]))
        df_sections.to_csv(sections_csv_path, encoding='utf-8-sig')
        df_data.to_csv(data_csv_path, encoding='utf-8-sig')
        maps_out_dir = date_filtered_path
    

    # Graph the reuse maps
    print("Creating graphs")
    graph_dir = os.path.join(out_dir, "graphs")
    if not os.path.exists(graph_dir):
        os.mkdir(graph_dir)
    for root, dirs, files in os.walk(maps_out_dir):
        for name in files:
            input_parts = name.split("-")
            text_name = ".".join(name.split(".")[:2])
            if input_parts[-1] == 'reuse.csv':
                reuse_map = os.path.join(root, name)
                section_map = os.path.join(root, "-".join(input_parts[:-1]) + "-section.csv")
                out_path = os.path.join(graph_dir, text_name + "reuse-graph.png")
                plot_reuse(reuse_map, out_path, text_name, section_map = section_map, add_dates = add_dates, top_colours=tops)



if __name__ == "__main__":
    corpus_base_path = "D:/OpenITI Corpus/corpus_2022_2_7/"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2022-2-7.csv"
    cluster_path = "D:/Corpus Stats/2023/v7-clusters/minified_clusters_pre-1000AH_under500.csv"
    # date_filter_range = [454, 467]
    in_books = ["0845Maqrizi.IghathaUmma"]
    out_dir = "../data_out_igatha_corrected/"
    new_ids_paths = [
        {"new_ids" : "C:/Users/mathe/Documents/Github-repos/clusters-analysis/create_md_uris/maqrizi.rasail_sections/0845Maqrizi.Rasail.clcluster-section-ids.csv",
         "new_ids_meta" : "C:/Users/mathe/Documents/Github-repos/clusters-analysis/create_md_uris/maqrizi.rasail_sections/0845Maqrizi.Rasail.clsection-ids-meta.csv"
         }
    ]
    existing_tagged_cluster = ["C:/Users/mathe/Documents/Github-repos/clusters-analysis/map_clusters_to_text/data_out_igatha_corrected/clusters_tagged/0845Maqrizi.IghathaUmma.Kraken210223142017-ara1.dyn-tagged"]

    topics = [{"id": "@PREIS@", "colour": "brown", "label" : "Pre-Islamic"},
          {"id": "@EARIS@", "colour": "yellow", "label" : "Early Islamic"},
          {"id": "@IKH@", "colour": "orange", "label" : "Ikhshidid"},
          {"id": "@FAT@", "colour": "green", "label" : "Fatimid"},
          {"id": "@AYY@", "colour": "red", "label" : "Ayyubid"},
          {"id": "@MAM@", "colour": "darkblue", "label" : "Mamluk"},
          {"id": "None", "colour": "black", "label" : "No Dynasty"}
          ]


    graph_for_dated_sections(in_books, out_dir, corpus_base_path, meta_path, cluster_path,  max_reuse_date=1000, 
                             cluster_cap = 20, date_summary=None, new_ids_paths = new_ids_paths, 
                             existing_cluster_tagged=existing_tagged_cluster, tops=topics)



    

        



