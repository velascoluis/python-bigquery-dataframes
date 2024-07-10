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
customer_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.customer")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")

date1 = date(1994, 11, 1)
date2 = date(1995, 2, 1)
osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
lsel = lineitem_ds.l_returnflag == "R"
forders = orders_ds[osel]
flineitem = lineitem_ds[lsel]
jn1 = flineitem.merge(forders, left_on="l_orderkey", right_on="o_orderkey")
jn2 = jn1.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
jn3 = jn2.merge(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
jn3["tmp"] = jn3.l_extendedprice * (1.0 - jn3.l_discount)
gb = jn3.groupby(
    [
        "c_custkey",
        "c_name",
        "c_acctbal",
        "c_phone",
        "n_name",
        "c_address",
        "c_comment",
    ],
    as_index=False,
)["tmp"].sum()
result_df = gb.sort_values("tmp", ascending=False)
print(result_df.head(5))
