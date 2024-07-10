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
""" customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
lineitem_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.lineitem")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")
orders_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.orders")
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
region_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.region")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

var1 = "BRAZIL"
var2 = "AMERICA"
var3 = "ECONOMY ANODIZED STEEL"
var4 = date(1995, 1, 1)
var5 = date(1996, 12, 31)
n1 = nation_ds.loc[:, ["n_nationkey", "n_regionkey"]]
n2 = nation_ds.loc[:, ["n_nationkey", "n_name"]]
jn1 = part_ds.merge(lineitem_ds, left_on="p_partkey", right_on="l_partkey")
jn2 = jn1.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
jn3 = jn2.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
jn4 = jn3.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
jn5 = jn4.merge(n1, left_on="c_nationkey", right_on="n_nationkey")
jn6 = jn5.merge(region_ds, left_on="n_regionkey", right_on="r_regionkey")
jn6 = jn6[(jn6["r_name"] == var2)]
jn7 = jn6.merge(n2, left_on="s_nationkey", right_on="n_nationkey")
jn7 = jn7[(jn7["o_orderdate"] >= var4) & (jn7["o_orderdate"] <= var5)]
jn7 = jn7[jn7["p_type"] == var3]
jn7["o_year"] = jn7["o_orderdate"].dt.year
jn7["volume"] = jn7["l_extendedprice"] * (1.0 - jn7["l_discount"])
jn7 = jn7.rename(columns={"n_name": "nation"})


@bpd.remote_function([float], float, reuse=False)
def udf(df) -> float:
    denonimator: float = df["volume"].sum()
    df = df[df["nation"] == var1]
    numerator: float = df["volume"].sum()
    return round(numerator / denonimator, 2)


gb = jn7.groupby("o_year", as_index=False)
agg = gb.apply(udf, include_groups=False)
agg = gb
agg.columns = ["o_year", "mkt_share"]
result_df = agg.sort_values("o_year")
# Add mean here, should change back
# result_df = agg.mean(numeric_only=True).sort_values("o_year")
print(result_df.head(5)) """
