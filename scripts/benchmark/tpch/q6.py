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

var1 = date(1994, 1, 1)
var2 = date(1995, 1, 1)
var3 = 0.05
var4 = 0.07
var5 = 24
filt = lineitem_ds[
    (lineitem_ds["l_shipdate"] >= var1) & (lineitem_ds["l_shipdate"] < var2)
]
filt = filt[(filt["l_discount"] >= var3) & (filt["l_discount"] <= var4)]
filt = filt[filt["l_quantity"] < var5]
result_value = (filt["l_extendedprice"] * filt["l_discount"]).sum()
result_df = bpd.DataFrame({"revenue": [result_value]})
print(result_df.head(5))
