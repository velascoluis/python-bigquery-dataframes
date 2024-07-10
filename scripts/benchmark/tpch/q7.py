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


nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")
customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

var1 = "FRANCE"
var2 = "GERMANY"
var3 = date(1995, 1, 1)
var4 = date(1996, 12, 31)
n1 = nation_ds[(nation_ds["n_name"] == var1)]
n2 = nation_ds[(nation_ds["n_name"] == var2)]
# Part 1
jn1 = customer_ds.merge(n1, left_on="c_nationkey", right_on="n_nationkey")
jn2 = jn1.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
jn2 = jn2.rename(columns={"n_name": "cust_nation"})
jn3 = jn2.merge(lineitem_ds, left_on="o_orderkey", right_on="l_orderkey")
jn4 = jn3.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
jn5 = jn4.merge(n2, left_on="s_nationkey", right_on="n_nationkey")
df1 = jn5.rename(columns={"n_name": "supp_nation"})
# Part 2
jn1 = customer_ds.merge(n2, left_on="c_nationkey", right_on="n_nationkey")
jn2 = jn1.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
jn2 = jn2.rename(columns={"n_name": "cust_nation"})
jn3 = jn2.merge(lineitem_ds, left_on="o_orderkey", right_on="l_orderkey")
jn4 = jn3.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
jn5 = jn4.merge(n1, left_on="s_nationkey", right_on="n_nationkey")
df2 = jn5.rename(columns={"n_name": "supp_nation"})
# Combine
total = bpd.concat([df1, df2])
total = total[(total["l_shipdate"] >= var3) & (total["l_shipdate"] <= var4)]
total["volume"] = total["l_extendedprice"] * (1.0 - total["l_discount"])
total["l_year"] = total["l_shipdate"].dt.year
gb = total.groupby(["supp_nation", "cust_nation", "l_year"], as_index=False)
agg = gb.agg(revenue=bpd.NamedAgg(column="volume", aggfunc="sum"))
result_df = agg.sort_values(by=["supp_nation", "cust_nation", "l_year"])
print(result_df.head(5))
