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


region_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.region")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
partsupp_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.partsupp")

var1 = 15
var2 = "BRASS"
var3 = "EUROPE"
jn = (
    part_ds.merge(partsupp_ds, left_on="p_partkey", right_on="ps_partkey")
    .merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
    .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
    .merge(region_ds, left_on="n_regionkey", right_on="r_regionkey")
)
jn = jn[jn["p_size"] == var1]
jn = jn[jn["p_type"].str.endswith(var2)]
jn = jn[jn["r_name"] == var3]
gb = jn.groupby("p_partkey", as_index=False)
agg = gb["ps_supplycost"].min()
jn2 = agg.merge(jn, on=["p_partkey", "ps_supplycost"])
sel = jn2.loc[
    :,
    [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ],
]
sort = sel.sort_values(
    by=["s_acctbal", "n_name", "s_name", "p_partkey"],
    ascending=[False, True, True, True],
)
result_df = sort.head(100)
print(result_df.head(5))
