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

startdate = date(1994, 3, 1)
enddate = date(1994, 4, 1)
p_type_like = "PROMO"
part_filtered = part_ds.loc[:, ["p_partkey", "p_type"]]
lineitem_filtered = lineitem_ds.loc[
    :, ["l_extendedprice", "l_discount", "l_shipdate", "l_partkey"]
]
sel = (lineitem_filtered.l_shipdate >= startdate) & (
    lineitem_filtered.l_shipdate < enddate
)
flineitem = lineitem_filtered[sel]
jn = flineitem.merge(part_filtered, left_on="l_partkey", right_on="p_partkey")
jn["tmp"] = jn.l_extendedprice * (1.0 - jn.l_discount)
result_df = jn[jn.p_type.str.startswith(p_type_like)].tmp.sum() * 100 / jn.tmp.sum()
print(result_df)
