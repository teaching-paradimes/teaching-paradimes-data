import glob
import json
import os

import pandas as pd
from tqdm import tqdm

from conf import col_mapping

files = sorted(glob.glob("./data/csv/*.csv"))

lookup_dict = {}
for x in files:
    heads, tail = os.path.split(x)
    df = pd.read_csv(x).fillna(0).astype(str)
    if tail.startswith("00"):
        continue
    else:
        name_parts = x.split("_")[2:]
        name = (
            "_".join(name_parts).lower().replace(".csv", "").replace(" ", "_")
        )
    lookup_dict[name] = {}
    df.columns = ["id", "value"]
    for i, row in df.iterrows():
        lookup_dict[name][str(row["id"])] = row.to_dict()
        lookup_dict[name][str(row["id"])]["new_id"] = i + 1

with open("helper_tables.json", "w", encoding="utf-8") as f:
    json.dump(lookup_dict, f, ensure_ascii=True, indent=2)

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
