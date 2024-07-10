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


customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")

customer_filtered = customer_ds.loc[:, ["c_custkey"]]
orders_filtered = orders_ds[
    ~orders_ds["o_comment"].str.contains(r"special[\s|\s]*requests")
]
orders_filtered = orders_filtered.loc[:, ["o_orderkey", "o_custkey"]]
c_o_merged = customer_filtered.merge(
    orders_filtered, left_on="c_custkey", right_on="o_custkey", how="left"
)
c_o_merged = c_o_merged.loc[:, ["c_custkey", "o_orderkey"]]
count_df = c_o_merged.groupby(["c_custkey"], as_index=False).agg(
    c_count=bpd.NamedAgg(column="o_orderkey", aggfunc="count")
)
total = count_df.groupby(["c_count"], as_index=False).size()
total.columns = ["c_count", "custdist"]
result_df = total.sort_values(by=["custdist", "c_count"], ascending=[False, False])
print(result_df.head(5))
