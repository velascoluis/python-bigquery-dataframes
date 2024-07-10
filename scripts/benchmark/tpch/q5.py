import os
import sys
import argparse

from datetime import date
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
customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

var1 = "ASIA"
var2 = date(1994, 1, 1)
var3 = date(1995, 1, 1)
jn1 = region_ds.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
jn3 = jn2.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
jn4 = jn3.merge(lineitem_ds, left_on="o_orderkey", right_on="l_orderkey")
jn5 = jn4.merge(
    supplier_ds,
    left_on=["l_suppkey", "n_nationkey"],
    right_on=["s_suppkey", "s_nationkey"],
)
jn5 = jn5[jn5["r_name"] == var1]
jn5 = jn5[(jn5["o_orderdate"] >= var2) & (jn5["o_orderdate"] < var3)]
jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
gb = jn5.groupby("n_name", as_index=False)["revenue"].sum()
result_df = gb.sort_values("revenue", ascending=False)
print(result_df.head(5))
