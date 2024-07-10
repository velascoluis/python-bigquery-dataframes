import os
import sys
import argparse

import bigframes.pandas as bpd

parser = argparse.ArgumentParser()
parser.add_argument("--size", type=str, required=True, help="Size of the benchmark")
args = parser.parse_args()

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
SIZE = args.size

if not PROJECT_ID:
    print(
        "Please set GOOGLE_CLOUD_PROJECT environment variable before running.",
        file=sys.stderr,
    )
    sys.exit(1)


part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
partsupp_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.partsupp")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

part_filtered = part_ds[
    (part_ds["p_brand"] != "brand#45")
    & (~part_ds["p_type"].str.contains("^MEDIUM POLISHED"))
    & part_ds["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9])
]
part_filtered = part_filtered.loc[:, ["p_partkey", "p_brand", "p_type", "p_size"]]
partsupp_filtered = partsupp_ds.loc[:, ["ps_partkey", "ps_suppkey"]]
total = part_filtered.merge(
    partsupp_filtered, left_on="p_partkey", right_on="ps_partkey", how="inner"
)
total = total.loc[:, ["p_brand", "p_type", "p_size", "ps_suppkey"]]
supplier_filtered = supplier_ds[
    supplier_ds["s_comment"].str.contains(r"customer(\s|\s)*complaints")
]
supplier_filtered = supplier_filtered.loc[:, ["s_suppkey"]].drop_duplicates()
# left merge to select only ps_suppkey values not in supplier_filtered
total = total.merge(
    supplier_filtered, left_on="ps_suppkey", right_on="s_suppkey", how="left"
)
total = total[total["s_suppkey"].isna()]
total = total.loc[:, ["p_brand", "p_type", "p_size", "ps_suppkey"]]
result_df = total.groupby(["p_brand", "p_type", "p_size"], as_index=False)[
    "ps_suppkey"
].nunique()
total.columns = ["p_brand", "p_type", "p_size", "supplier_cnt"]
total = total.sort_values(
    by=["supplier_cnt", "p_brand", "p_type", "p_size"],
    ascending=[False, True, True, True],
)
print(result_df.head(5))
