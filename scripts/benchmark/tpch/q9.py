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
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")
partsupp_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.partsupp")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

psel = part_ds.p_name.str.contains("ghost")
fpart = part_ds[psel]
jn1 = lineitem_ds.merge(fpart, left_on="l_partkey", right_on="p_partkey")
jn2 = jn1.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
jn3 = jn2.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
jn4 = partsupp_ds.merge(
    jn3,
    left_on=["ps_partkey", "ps_suppkey"],
    right_on=["l_partkey", "l_suppkey"],
)
jn5 = jn4.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
jn5["tmp"] = jn5.l_extendedprice * (1 - jn5.l_discount) - (
    (1 * jn5.ps_supplycost) * jn5.l_quantity
)
jn5["o_year"] = jn5.o_orderdate.dt.year
gb = jn5.groupby(["n_name", "o_year"], as_index=False)["tmp"].sum()
result_df = gb.sort_values(["n_name", "o_year"], ascending=[True, False])
print(result_df.head(5))
