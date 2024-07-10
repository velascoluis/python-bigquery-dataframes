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


lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")

left = lineitem_ds.loc[:, ["l_partkey", "l_quantity", "l_extendedprice"]]
right = part_ds[
    ((part_ds["p_brand"] == "Brand#23") & (part_ds["p_container"] == "MED BOX"))
]
right = right.loc[:, ["p_partkey"]]
line_part_merge = left.merge(
    right, left_on="l_partkey", right_on="p_partkey", how="inner"
)
line_part_merge = line_part_merge.loc[:, ["l_quantity", "l_extendedprice", "p_partkey"]]
lineitem_filtered = lineitem_ds.loc[:, ["l_partkey", "l_quantity"]]
lineitem_avg = lineitem_filtered.groupby(["l_partkey"], as_index=False).agg(
    avg=bpd.NamedAgg(column="l_quantity", aggfunc="mean")
)
lineitem_avg["avg"] = 0.2 * lineitem_avg["avg"]
lineitem_avg = lineitem_avg.loc[:, ["l_partkey", "avg"]]
total = line_part_merge.merge(
    lineitem_avg, left_on="p_partkey", right_on="l_partkey", how="inner"
)
total = total[total["l_quantity"] < total["avg"]]
result_df = bpd.DataFrame({"avg_yearly": [total["l_extendedprice"].sum() / 7.0]})
print(result_df.head(5))
