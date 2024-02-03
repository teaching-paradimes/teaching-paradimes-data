import json
import os

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

existing_tables = client.fetch_table_field_dict(BASEROW_DB)
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
print(f"now lets create coursed table")


COURSES_SCHEMA = [
    {"name": "course_title", "type": "long_text"},
    {"name": "course_type", "type": "link_row", "link_row_table_id": table_ids["course_type"]["id"]},
    {"name": "instructor", "type": "text"},
    {"name": "institute", "type": "link_row", "link_row_table_id": table_ids["institute"]["id"]},
    {"name": "university", "type": "link_row", "link_row_table_id": table_ids["university"]["id"]},
    {"name": "country", "type": "link_row", "link_row_table_id": table_ids["country"]["id"]},
    {"name": "discipline", "type": "link_row", "link_row_table_id": table_ids["discipline"]["id"]},
    {"name": "discipline", "type": "link_row", "link_row_table_id": table_ids["discipline"]["id"]},
    {"name": "subdiscipline", "type": "link_row", "link_row_table_id": table_ids["subdiscipline"]["id"]},
    {"name": "course_category", "type": "link_row", "link_row_table_id": table_ids["course_category"]["id"]},
    {"name": "aws_specifics_period", "type": "link_row", "link_row_table_id": table_ids["aws_specifics_period"]["id"]},
    {"name": "aws_specifics_evidence", "type": "link_row", "link_row_table_id": table_ids["aws_specifics_evidence"]["id"]},
    {"name": "aws_specifics_space", "type": "link_row", "link_row_table_id": table_ids["aws_specifics_space"]["id"]},
]

courses, created = client.create_table("courses")
field, created = client.create_table_fields(courses["id"], COURSES_SCHEMA)
client.delete_table_fields(courses["id"], ["Name", "Notes", "Active"])

print("done with courses table")