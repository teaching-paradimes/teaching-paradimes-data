import json
import os

import more_itertools as mit
import requests
from acdh_baserow_pyutils import BaseRowClient
from tqdm import tqdm

BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN")
BASEROW_USER = os.environ.get("BASEROW_USER")
BASEROW_PW = os.environ.get("BASEROW_PW")
BASEROW_URL = os.environ.get("BASEROW_URL")
BASEROW_DB = os.environ.get("BASEROW_DB")

client = BaseRowClient(
    BASEROW_USER,
    BASEROW_PW,
    BASEROW_TOKEN,
    br_base_url=BASEROW_URL,
    br_db_id=BASEROW_DB,
)

courses_table_id = client.br_table_dict["courses"]["id"]

with open("tmp.json", "r") as f:
    data = json.load(f)

batched_data = list(mit.chunked(data, 199))

sample_fields = [
    "course_title",
    "course_type",
    "semester",
    "instructor",
    "institute",
    "university",
    "country",
    "notes",
    "discipline",
    "subdiscipline",
    "course_category",
]
update_url = f"{client.br_base_url}database/rows/table/{courses_table_id}/batch/?user_field_names=true"
for batch in tqdm(batched_data):
    objects = []
    for x in batch:
        item = {}
        for field in sample_fields:
            if isinstance(x[field], dict):
                try:
                    item[field] = [int(x[field]["id"])]
                except KeyError:
                    pass
            else:
                item[field] = x[field]
        objects.append(item)
    r = requests.post(
        update_url,
        headers={
            "Authorization": f"Token {client.br_token}",
            "Content-Type": "application/json",
        },
        json={"items": objects},
    )
