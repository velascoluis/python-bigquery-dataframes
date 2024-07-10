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


lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")

var1 = date(1993, 7, 1)
var2 = date(1993, 10, 1)
jn = lineitem_ds.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
jn = jn[(jn["o_orderdate"] >= var1) & (jn["o_orderdate"] < var2)]
jn = jn[jn["l_commitdate"] < jn["l_receiptdate"]]
jn = jn.drop_duplicates(subset=["o_orderpriority", "l_orderkey"])
gb = jn.groupby("o_orderpriority", as_index=False)
agg = gb.agg(order_count=bpd.NamedAgg(column="o_orderkey", aggfunc="count"))
result_df = agg.sort_values(["o_orderpriority"])
print(result_df.head(5))
