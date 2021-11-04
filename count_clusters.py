
import pandas as pd
import os

def count_clusters(path):
    os.chdir(path)
    unique_all = []
    for root, dirs, files in os.walk(".", topdown=False):
        
        for name in files:
            json_path = os.path.join(root, name)
            print(json_path)
            listed = pd.read_json(json_path, lines = True)["cluster"]
            unique = list(dict.fromkeys(listed))
            unique_all.extend(unique)
    
    unique_all = list(dict.fromkeys(unique_all))
    
    return len(unique_all)
            
            


json_path = "D:/Corpus Stats/2021/Cluster data/full_clusters/out.json"
count = count_clusters(json_path)          