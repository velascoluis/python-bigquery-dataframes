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

# Query fails
""" lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")

date1 = date(1994, 1, 1)
date2 = date(1995, 1, 1)
sel = (
    (lineitem_ds.l_receiptdate < date2)
    & (lineitem_ds.l_commitdate < date2)
    & (lineitem_ds.l_shipdate < date2)
    & (lineitem_ds.l_shipdate < lineitem_ds.l_commitdate)
    & (lineitem_ds.l_commitdate < lineitem_ds.l_receiptdate)
    & (lineitem_ds.l_receiptdate >= date1)
    & ((lineitem_ds.l_shipmode == "mail") | (lineitem_ds.l_shipmode == "ship"))
)
flineitem = lineitem_ds[sel]
jn = flineitem.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")


def g1(x):
    return ((x == "1-URGENT") | (x == "2-HIGH")).sum()


def g2(x):
    return ((x != "1-URGENT") & (x != "2-HIGH")).sum()


total = jn.groupby("l_shipmode", as_index=False)["o_orderpriority"].agg((g1, g2))
total = total.reset_index()
result_df = total.sort_values("l_shipmode")
print(result_df.head(5)) """
