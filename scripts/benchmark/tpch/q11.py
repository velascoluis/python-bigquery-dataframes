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


partsupp_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.partsupp")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")

partsupp_filtered = partsupp_ds.loc[:, ["ps_partkey", "ps_suppkey"]]
partsupp_filtered["total_cost"] = (
    partsupp_ds["ps_supplycost"] * partsupp_ds["ps_availqty"]
)
supplier_filtered = supplier_ds.loc[:, ["s_suppkey", "s_nationkey"]]
ps_supp_merge = partsupp_filtered.merge(
    supplier_filtered, left_on="ps_suppkey", right_on="s_suppkey", how="inner"
)
ps_supp_merge = ps_supp_merge.loc[:, ["ps_partkey", "s_nationkey", "total_cost"]]
nation_filtered = nation_ds[(nation_ds["n_name"] == "GERMANY")]
nation_filtered = nation_filtered.loc[:, ["n_nationkey"]]
ps_supp_n_merge = ps_supp_merge.merge(
    nation_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner"
)
ps_supp_n_merge = ps_supp_n_merge.loc[:, ["ps_partkey", "total_cost"]]
sum_val = ps_supp_n_merge["total_cost"].sum() * 0.0001
total = ps_supp_n_merge.groupby(["ps_partkey"], as_index=False).agg(
    value=bpd.NamedAgg(column="total_cost", aggfunc="sum")
)
total = total[total["value"] > sum_val]
result_df = total.sort_values("value", ascending=False)
print(result_df.head(5))
