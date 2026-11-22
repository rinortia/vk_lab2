import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, LongType, NestedField, StringType


def schema(fields):
    return Schema(*[NestedField(i, name, typ, required=False)
                    for i, (name, typ) in enumerate(fields, 1)])


RAW_SCHEMA = schema([
    ("event_id", StringType()), ("source_id", StringType()), ("topic", StringType()),
    ("partition", LongType()), ("offset", LongType()), ("account_id", LongType()),
    ("owner", StringType()), ("city", StringType()), ("status", StringType()),
    ("op", StringType()), ("event_time_ms", LongType()), ("lsn", LongType()),
    ("raw_json", StringType()),
])
DIM_SCHEMA = schema([
    ("version_id", StringType()), ("account_id", LongType()), ("version", LongType()),
    ("owner", StringType()), ("city", StringType()), ("status", StringType()),
    ("is_deleted", BooleanType()), ("valid_from_ms", LongType()),
    ("valid_to_ms", LongType()), ("is_current", BooleanType()),
    ("partition", LongType()), ("kafka_offset", LongType()), ("lsn", LongType()),
])


def catalog():
    props = {
        "type": "sql", "uri": os.environ.get("CATALOG_URI", "postgresql+psycopg2://catalog:catalog@catalog:5432/catalog"),
        "warehouse": os.environ.get("WAREHOUSE", "s3://warehouse/"),
    }
    if os.environ.get("S3_ENDPOINT"):
        props.update({
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.region": "us-east-1", "s3.force-virtual-addressing": "false",
        })
    return load_catalog("lab", **props)


def tables():
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    cat = catalog()
    try:
        cat.create_namespace("bank")
    except NamespaceAlreadyExistsError:
        pass
    result = []
    for name, definition in [("bank.cdc_events", RAW_SCHEMA), ("bank.accounts_scd2", DIM_SCHEMA)]:
        try:
            table = cat.load_table(name)
        except NoSuchTableError:
            table = cat.create_table(name, definition, properties={"format-version": "2"})
        result.append(table)
    return result


def write(table, rows, overwrite=False):
    import pyarrow as pa
    data = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    if overwrite:
        table.overwrite(data)
    elif rows:
        table.append(data)
