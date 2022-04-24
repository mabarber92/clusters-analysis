# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 18:31:07 2022

@author: mathe
"""

import seaborn as sns
import pandas as pd

df_path = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/freq-to-20.csv"
data = pd.read_csv(df_path)[:-1]
data = data[["Cluster Size", "Count"]]
print(data)

figure_out = "C:/Users/mathe/Documents/Github-repos/clusters-analysis/freq-bar.png"

ticks = [500, 1000, 5000, 10000, 50000, 100000, 500000]
sns.set_theme(style = "whitegrid")
g = sns.barplot(data = data, x = "Cluster Size", y = "Count", color = "lightblue")
g.set_yscale("log")
g.set_yticks(ticks)
g.set_yticklabels(ticks)
fig = g.get_figure()
fig.savefig(figure_out, dpi = 300, bbox_inches = 'tight')