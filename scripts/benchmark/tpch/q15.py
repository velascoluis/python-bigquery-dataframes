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
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

lineitem_filtered = lineitem_ds[
    (lineitem_ds["l_shipdate"] >= date(1996, 1, 1))
    & (lineitem_ds["l_shipdate"] < (date(1996, 4, 1)))
]
lineitem_filtered["revenue_parts"] = lineitem_filtered["l_extendedprice"] * (
    1.0 - lineitem_filtered["l_discount"]
)
lineitem_filtered = lineitem_filtered.loc[:, ["l_suppkey", "revenue_parts"]]
revenue_table = (
    lineitem_filtered.groupby("l_suppkey", as_index=False)
    .agg(total_revenue=bpd.NamedAgg(column="revenue_parts", aggfunc="sum"))
    .rename(columns={"l_suppkey": "supplier_no"})
)
max_revenue = revenue_table["total_revenue"].max()
revenue_table = revenue_table[revenue_table["total_revenue"] == max_revenue]
supplier_filtered = supplier_ds.loc[:, ["s_suppkey", "s_name", "s_address", "s_phone"]]
total = supplier_filtered.merge(
    revenue_table, left_on="s_suppkey", right_on="supplier_no", how="inner"
)
result_df = total.loc[
    :, ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
]
print(result_df.head(5))
