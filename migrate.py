import json
import os

import requests
from acdh_baserow_pyutils import BaseRowClient

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

existing_tables = client.br_table_dict
print("delete existing tables")

for key, value in existing_tables.items():
    table_id = value["id"]
    print(client.delete_table(str(table_id)))

with open("helper_tables.json", "r") as f:
    tables = json.load(f)

print("creating and populating table")
table_ids = {}
for key, value in tables.items():
    fields = [[x["value"]] for y, x in value.items() if x["value"] != "0"]
    fields.insert(0, ["value"])
    table, created = client.create_table(key, fields=fields)
    print(f"created {table}")
    if created:
        table_ids[key] = table
print(f"done, created {len(table_ids)} tables")
print("now lets create courses table")


COURSES_SCHEMA = [
    {
        "name": "course_type",
        "type": "link_row",
        "link_row_table_id": table_ids["course_type"]["id"],
        "has_related_field": False,
    },
    {"name": "instructor", "type": "text"},
    {
        "name": "institute",
        "type": "link_row",
        "link_row_table_id": table_ids["institute"]["id"],
        "has_related_field": False,
    },
    {
        "name": "university",
        "type": "link_row",
        "link_row_table_id": table_ids["university"]["id"],
        "has_related_field": False,
    },
    {
        "name": "country",
        "type": "link_row",
        "link_row_table_id": table_ids["country"]["id"],
        "has_related_field": False,
    },
    {
        "name": "semester",
        "type": "link_row",
        "link_row_table_id": table_ids["semester"]["id"],
        "has_related_field": False,
    },
    {
        "name": "discipline",
        "type": "link_row",
        "link_row_table_id": table_ids["discipline"]["id"],
        "has_related_field": False,
    },
    {
        "name": "subdiscipline",
        "type": "link_row",
        "link_row_table_id": table_ids["subdiscipline"]["id"],
        "has_related_field": False,
    },
    {
        "name": "course_category",
        "type": "link_row",
        "link_row_table_id": table_ids["course_category"]["id"],
        "has_related_field": False,
    },
    {
        "name": "aws_specifics_evidence",
        "type": "link_row",
        "link_row_table_id": table_ids["aws_specifics_evidence"]["id"],
        "has_related_field": False,
    },
    {
        "name": "aws_specifics_period",
        "type": "link_row",
        "link_row_table_id": table_ids["aws_specifics_period"]["id"],
        "has_related_field": False,
    },
    {
        "name": "aws_specifics_space",
        "type": "link_row",
        "link_row_table_id": table_ids["aws_specifics_space"]["id"],
        "has_related_field": False,
    },
    {
        "name": "aws_specifics_themes",
        "type": "link_row",
        "link_row_table_id": table_ids["aws_specifics_themes"]["id"],
        "has_related_field": False,
    },
]

courses, created = client.create_table("courses")
field, created = client.create_table_fields(courses["id"], COURSES_SCHEMA)
print("renaming first column")
client = BaseRowClient(
    BASEROW_USER,
    BASEROW_PW,
    BASEROW_TOKEN,
    br_base_url=BASEROW_URL,
    br_db_id=BASEROW_DB,
)
name_field = client.br_table_dict["courses"]["fields"]["Name"]
url = f"{client.br_base_url}database/fields/{name_field['id']}/"
r = requests.patch(
    url,
    headers={
        "Authorization": f"JWT {client.br_jwt_token}",
        "Content-Type": "application/json",
    },
    json={"name": "course_title"},
)
print(f"{r.content.decode('utf-8')}")
client.delete_table_fields(courses["id"], ["Notes", "Active"])

print("done with courses table")
