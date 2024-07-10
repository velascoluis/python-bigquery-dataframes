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
part_ds = bpd.read_gbq(f"{PROJECT_ID}.tpch_{SIZE}.part")
# Query fails 
""" Brand31 = "Brand#31"
Brand43 = "Brand#43"
SMBOX = "SM BOX"
SMCASE = "SM CASE"
SMPACK = "SM PACK"
SMPKG = "SM PKG"
MEDBAG = "MED BAG"
MEDBOX = "MED BOX"
MEDPACK = "MED PACK"
MEDPKG = "MED PKG"
LGBOX = "LG BOX"
LGCASE = "LG CASE"
LGPACK = "LG PACK"
LGPKG = "LG PKG"
DELIVERINPERSON = "DELIVER IN PERSON"
AIR = "AIR"
AIRREG = "AIRREG"
lsel = (
    (
        ((lineitem_ds.l_quantity <= 36) & (lineitem_ds.l_quantity >= 26))
        | ((lineitem_ds.l_quantity <= 25) & (lineitem_ds.l_quantity >= 15))
        | ((lineitem_ds.l_quantity <= 14) & (lineitem_ds.l_quantity >= 4))
    )
    & (lineitem_ds.l_shipinstruct == DELIVERINPERSON)
    & ((lineitem_ds.l_shipmode == AIR) | (lineitem_ds.l_shipmode == AIRREG))
)
psel = (part_ds.p_size >= 1) & (
    (
        (part_ds.p_size <= 5)
        & (part_ds.p_brand == Brand31)
        & (
            (part_ds.p_container == SMBOX)
            | (part_ds.p_container == SMCASE)
            | (part_ds.p_container == SMPACK)
            | (part_ds.p_container == SMPKG)
        )
    )
    | (
        (part_ds.p_size <= 10)
        & (part_ds.p_brand == Brand43)
        & (
            (part_ds.p_container == MEDBAG)
            | (part_ds.p_container == MEDBOX)
            | (part_ds.p_container == MEDPACK)
            | (part_ds.p_container == MEDPKG)
        )
    )
    | (
        (part_ds.p_size <= 15)
        & (part_ds.p_brand == Brand43)
        & (
            (part_ds.p_container == LGBOX)
            | (part_ds.p_container == LGCASE)
            | (part_ds.p_container == LGPACK)
            | (part_ds.p_container == LGPKG)
        )
    )
)
flineitem = lineitem_ds[lsel]
fpart = part_ds[psel]
jn = flineitem.merge(fpart, left_on="l_partkey", right_on="p_partkey")
jnsel = (
    (jn.p_brand == Brand31)
    & (
        (jn.p_container == SMBOX)
        | (jn.p_container == SMCASE)
        | (jn.p_container == SMPACK)
        | (jn.p_container == SMPKG)
    )
    & (jn.l_quantity >= 4)
    & (jn.l_quantity <= 14)
    & (jn.p_size <= 5)
    | (jn.p_brand == Brand43)
    & (
        (jn.p_container == MEDBAG)
        | (jn.p_container == MEDBOX)
        | (jn.p_container == MEDPACK)
        | (jn.p_container == MEDPKG)
    )
    & (jn.l_quantity >= 15)
    & (jn.l_quantity <= 25)
    & (jn.p_size <= 10)
    | (jn.p_brand == Brand43)
    & (
        (jn.p_container == LGBOX)
        | (jn.p_container == LGCASE)
        | (jn.p_container == LGPACK)
        | (jn.p_container == LGPKG)
    )
    & (jn.l_quantity >= 26)
    & (jn.l_quantity <= 36)
    & (jn.p_size <= 15)
)
jn = jn[jnsel]
result_df = (jn.l_extendedprice * (1.0 - jn.l_discount)).sum()
print(result_df.head(5)) """
