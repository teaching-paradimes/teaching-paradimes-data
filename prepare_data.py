import glob
import json
import os

import pandas as pd
from tqdm import tqdm

col_mapping = {
    "aws_period_1": "aws_specifics_period",
    "aws_period_2": "aws_specifics_period",
    "aws_space_1": "aws_specifics_space",
    "aws_space_2": "aws_specifics_space",
    "aws_evidence_1": "aws_specifics_evidence",
    "aws_evidence_2": "aws_specifics_evidence",
    "aws_theme_1": "aws_specifics_themes",
    "aws_theme_2": "aws_specifics_themes",
    "smt_specifics_1": "smt_specifics",
    "smt_specifics_2": "smt_specifics",
    "ha_specifics": "ha_specifics",
    "as_specifics": "as_specifics",
    "rs_specifics": "rs_specifics",
}


files = sorted(glob.glob("./data/csv/*.csv"))

lookup_dict = {}
for x in files:
    heads, tail = os.path.split(x)
    df = pd.read_csv(x).fillna(0).astype(str)
    if tail.startswith("00"):
        continue
    else:
        name_parts = x.split("_")[2:]
        name = "_".join(name_parts).lower().replace(".csv", "").replace(" ", "_")
    lookup_dict[name] = {}
    df.columns = ["id", "value"]
    for i, row in df.iterrows():
        lookup_dict[name][str(row["id"])] = row.to_dict()

df = pd.read_csv(files[0]).fillna(0).astype(str)
df.columns = [x.replace(" ", "_") for x in df.columns.str.lower()]
df = df.rename(columns={"course_general_category": "course_category"})


data = []
for i, row in tqdm(df.iterrows()):
    item = row.to_dict()
    for x in df.keys():
        try:
            lookup_match = lookup_dict[x]
        except KeyError:
            item[x] = row[x]
            continue
        cur_id = str(row[x]).replace(".0", "")
        try:
            item[x] = lookup_match[cur_id]
        except KeyError:
            item[x] = row[x]
    data.append(item)

for x in data:
    for key, value in x.items():
        if key == "id":
            continue
        if isinstance(value, dict):
            continue
        if value.endswith(".0") and value != "0.0":
            value = value.replace(".0", "")
            try:
                new_value = lookup_dict[col_mapping[key]][value]
                x[key] = new_value
            except KeyError:
                pass
        if value == "0.0":
            x[key] = {}

with open("tmp.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
