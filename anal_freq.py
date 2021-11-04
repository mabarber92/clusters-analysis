# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 08:51:26 2021

@author: mathe
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

path = "freq_out/cluster_freqs.csv"

data = pd.read_csv(path)

# print("scatter")
# plt.figure()
# plot = sns.scatterplot(data=data, x="Ms_count", y="Book_count")
# plot.set(xscale = "log", yscale = "log")


# plt.savefig("plots/Scatter_log.png", dpi=300)

# print("box1")
# plt.figure()
# hist = sns.boxplot(x=data["Ms_count"], showfliers=False)

# plt.savefig("plots/ms_dist.png", dpi=300)

# print("box2")
# plt.figure()
# hist2 = sns.boxplot(x = data["Book_count"], showfliers=False)

# plt.savefig("plots/book_dict.png", dpi=300)

data["diff"] = data["Ms_count"] - data["Book_count"]

# print("scatter2")
# plt.figure()
# plot = sns.scatterplot(data=data, x="Cluster", y="diff")

# plt.savefig("plots/diff_plot", dpi=300)

trial_values = [7, 100, 500]

for v in trial_values:
    data_filtered = data.loc[data["Ms_count"] > v]
    data_filtered.to_csv("clusters-over-" +str(v) + ".csv", index = False)

data_filtered_diff = data.loc[data["diff"] > 0]
data_filtered_diff.to_csv("over-0-diff.csv", index=False)


