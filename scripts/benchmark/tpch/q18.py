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
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")
customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")

gb1 = lineitem_ds.groupby("l_orderkey", as_index=False)["l_quantity"].sum()
fgb1 = gb1[gb1.l_quantity > 300]
jn1 = fgb1.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
jn2 = jn1.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
gb2 = jn2.groupby(
    ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"],
    as_index=False,
)["l_quantity"].sum()
result_df = gb2.sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
print(result_df.head(5))
