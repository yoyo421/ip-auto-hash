import pandas as pd
import glob
import numpy as np
import sys

md = sys.argv[1] if len(sys.argv) > 1 else None

csvs = sorted(glob.glob("sample_hash*.csv*"))
df_list = [pd.read_csv(csv, compression='gzip' if '.gzip' in csv else None) for csv in csvs]

df_conclusion = pd.DataFrame(index=csvs, columns=pd.DataFrame([[1]]).describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999]).index)
df_slow_conclusion = pd.DataFrame(index=csvs, columns=pd.DataFrame([[1]]).describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999]).index)
total_durations = pd.concat([df['duration_ns'] for df in df_list], ignore_index=True)
# Derive common bin edges from all data, then bin each CSV using those same edges.
# pd.cut(..., retbins=True) returns (categorical, bins)
_, bin_edges = pd.qcut(total_durations, q=20, duplicates='drop', retbins=True)
df_bins = pd.DataFrame(index=csvs, columns=pd.IntervalIndex.from_breaks(bin_edges))

for csv, df in zip(csvs, df_list):
    df_conclusion.loc[csv, :] = df['duration_ns'].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999])
    # Use numpy histogram with the common bin edges to get counts per bin
    counts, _ = np.histogram(df['duration_ns'], bins=bin_edges)
    df_bins.loc[csv, :] = counts

    sorted_dns = df['duration_ns'].sort_values(ascending=True)
    length = len(sorted_dns)
    df_slow_conclusion.loc[csv, :] = sorted_dns[(length * 3)//4:].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999])

if md:
    print("=== Slow Summary Statistics (Top 25%) ===")
    print(df_slow_conclusion.to_markdown())
    print("\n=== Summary Statistics ===")
    print(df_conclusion.to_markdown())
    print("\n=== Histogram Bins ===")
    print(df_bins.T.to_markdown())
else:
    print("=== Slow Summary Statistics (Top 25%) ===")
    print(df_slow_conclusion)
    print("\n=== Summary Statistics ===")
    print(df_conclusion)
    print("\n=== Histogram Bins ===")
    print(df_bins.T)