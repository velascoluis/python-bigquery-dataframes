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


customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")

var1 = "BUILDING"
var2 = date(1995, 3, 15)
fcustomer = customer_ds[customer_ds["c_mktsegment"] == var1]
jn1 = fcustomer.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
jn2 = jn1.merge(lineitem_ds, left_on="o_orderkey", right_on="l_orderkey")
jn2 = jn2[jn2["o_orderdate"] < var2]
jn2 = jn2[jn2["l_shipdate"] > var2]
jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)
gb = jn2.groupby(["o_orderkey", "o_orderdate", "o_shippriority"], as_index=False)
agg = gb["revenue"].sum()
sel = agg.loc[:, ["o_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
sel = sel.rename(columns={"o_orderkey": "l_orderkey"})
sorted = sel.sort_values(by=["revenue", "o_orderdate"], ascending=[False, True])
result_df = sorted.head(10)
print(result_df.head(5))
