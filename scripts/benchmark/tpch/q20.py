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
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
nation_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.nation")
partsupp_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.partsupp")
supplier_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.supplier")

date1 = date(1996, 1, 1)
date2 = date(1997, 1, 1)
psel = part_ds.p_name.str.startswith("azure")
nsel = nation_ds.n_name == "JORDAN"
lsel = (lineitem_ds.l_shipdate >= date1) & (lineitem_ds.l_shipdate < date2)
fpart = part_ds[psel]
fnation = nation_ds[nsel]
flineitem = lineitem_ds[lsel]
jn1 = fpart.merge(partsupp_ds, left_on="p_partkey", right_on="ps_partkey")
jn2 = jn1.merge(
    flineitem,
    left_on=["ps_partkey", "ps_suppkey"],
    right_on=["l_partkey", "l_suppkey"],
)
gb = jn2.groupby(["ps_partkey", "ps_suppkey", "ps_availqty"], as_index=False)[
    "l_quantity"
].sum()
gbsel = gb.ps_availqty > (0.5 * gb.l_quantity)
fgb = gb[gbsel]
jn3 = fgb.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
jn4 = fnation.merge(jn3, left_on="n_nationkey", right_on="s_nationkey")
jn4 = jn4.loc[:, ["s_name", "s_address"]]
result_df = jn4.sort_values("s_name").drop_duplicates()
print(result_df.head(5))
