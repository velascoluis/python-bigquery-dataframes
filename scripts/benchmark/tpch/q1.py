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

var1 = date(1998, 9, 2)
filt = lineitem_ds[lineitem_ds["l_shipdate"] <= var1]
filt["disc_price"] = filt.l_extendedprice * (1.0 - filt.l_discount)
filt["charge"] = filt.l_extendedprice * (1.0 - filt.l_discount) * (1.0 + filt.l_tax)
gb = filt.groupby(["l_returnflag", "l_linestatus"], as_index=False)
agg = gb.agg(
    sum_qty=bpd.NamedAgg("l_quantity", "sum"),
    sum_base_price=bpd.NamedAgg("l_extendedprice", "sum"),
    sum_disc_price=bpd.NamedAgg("disc_price", "sum"),
    sum_charge=bpd.NamedAgg("charge", "sum"),
    avg_qty=bpd.NamedAgg("l_quantity", "mean"),
    avg_price=bpd.NamedAgg("l_extendedprice", "mean"),
    avg_disc=bpd.NamedAgg("l_discount", "mean"),
    count_order=bpd.NamedAgg("l_orderkey", "count"),
)
result_df = agg.sort_values(["l_returnflag", "l_linestatus"])
print(result_df.head(5))
bpd.reset_session()
