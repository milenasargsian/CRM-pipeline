"""
CRM Data Ingestion Script
Loads CSV files from ./data/ into ClickHouse Cloud landing layer.
"""

import os
import logging
import pandas as pd
import clickhouse_connect


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Connection
CH_CONFIG = {
    "host":     "bognfyt333.germanywestcentral.azure.clickhouse.cloud",
    "user":     "default",
    "password": "yb1dBVPsos~3H",
    "database": "crm",
}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# File to Table mapping
FILE_CONFIG = {
    "users.csv": {
        "table": "lnd_users",
        "rename": {},
        "bool_cols": [],
    },
    "stages.csv": {
        "table": "lnd_stages",
        "rename": {},
        "bool_cols": [],
    },
    "fields.csv": {
        "table": "lnd_fields",
        "rename": {
            "ID": "id",
            "FIELD_KEY": "field_key",
            "NAME": "name",
            "FIELD_VALUE_OPTIONS": "field_value_options",
        },
        "bool_cols": [],
    },
    "deal_changes.csv": {
        "table": "lnd_deal_changes",
        "rename": {},
        "bool_cols": [],
    },
    "activity.csv": {
        "table": "lnd_activity",
        "rename": {},
        "bool_cols": ["done"],
    },
    "activity_types.csv": {
        "table": "lnd_activity_types",
        "rename": {},
        "bool_cols": [],
    },
}

# Integer columns
INT_COLS = {
    "lnd_users":          ["id"],
    "lnd_stages":         ["stage_id"],
    "lnd_fields":         ["id"],
    "lnd_deal_changes":   ["deal_id"],
    "lnd_activity":       ["activity_id", "assigned_to_user", "deal_id"],
    "lnd_activity_types": ["id"],
}


def get_client():
    return clickhouse_connect.get_client(
        host=CH_CONFIG["host"],
        user=CH_CONFIG["user"],
        password=CH_CONFIG["password"],
        secure=True,
    )


def setup_database(client):
    """
        Create database and landing tables if they don't exist.
    """
    client.command("CREATE DATABASE IF NOT EXISTS crm")

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_users (
            id        Int32,
            name      String,
            email     String,
            modified  String
        ) ENGINE = MergeTree() ORDER BY id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_stages (
            stage_id    Int32,
            stage_name  String
        ) ENGINE = MergeTree() ORDER BY stage_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_fields (
            id                   Int32,
            field_key            String,
            name                 String,
            field_value_options  String
        ) ENGINE = MergeTree() ORDER BY id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_deal_changes (
            deal_id           Int32,
            change_time       String,
            changed_field_key String,
            new_value         String
        ) ENGINE = MergeTree() ORDER BY (deal_id, change_time)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_activity (
            activity_id       Int32,
            type              String,
            assigned_to_user  Int32,
            deal_id           Int32,
            done              UInt8,
            due_to            String
        ) ENGINE = MergeTree() ORDER BY activity_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS crm.lnd_activity_types (
            id      Int32,
            name    String,
            active  String,
            type    String
        ) ENGINE = MergeTree() ORDER BY id
    """)

    log.info("Database and landing tables are ready")


def load_file(client, filename, cfg):
    filepath = os.path.join(DATA_DIR, filename)
    log.info("Loading %s", filename)

    df = pd.read_csv(filepath, dtype=str).fillna("")

    if cfg["rename"]:
        df = df.rename(columns=cfg["rename"])

    # Boolean columns
    for col in cfg["bool_cols"]:
        if col in df.columns:
            df[col] = df[col].str.lower().map({"true": 1, "false": 0}).fillna(0).astype(int)

    # Integer columns
    for col in INT_COLS.get(cfg["table"], []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    client.command(f"TRUNCATE TABLE crm.{cfg['table']}")
    client.insert_df(f"crm.{cfg['table']}", df)
    log.info("%d rows -> crm.%s", len(df), cfg["table"])


def main():
    log.info("Connecting to ClickHouse Cloud")
    client = get_client()

    log.info("Setting up database and tables")
    setup_database(client)

    log.info("Loading CSV files")
    for filename, cfg in FILE_CONFIG.items():
        load_file(client, filename, cfg)

    log.info("\nIngestion complete")


if __name__ == "__main__":
    main()


# Output

# 2026-04-05 18:19:25,132 INFO Connecting to ClickHouse Cloud
# 2026-04-05 18:19:41,114 INFO Setting up database and tables
# 2026-04-05 18:19:42,259 INFO Database and landing tables are ready
# 2026-04-05 18:19:42,259 INFO Loading CSV files
# 2026-04-05 18:19:42,260 INFO Loading users.csv
# 2026-04-05 18:19:42,735 INFO 1787 rows -> crm.lnd_users
# 2026-04-05 18:19:42,735 INFO Loading stages.csv
# 2026-04-05 18:19:43,025 INFO 9 rows -> crm.lnd_stages
# 2026-04-05 18:19:43,025 INFO Loading fields.csv
# 2026-04-05 18:19:43,320 INFO 4 rows -> crm.lnd_fields
# 2026-04-05 18:19:43,321 INFO Loading deal_changes.csv
# 2026-04-05 18:19:43,840 INFO 15406 rows -> crm.lnd_deal_changes
# 2026-04-05 18:19:43,841 INFO Loading activity.csv
# 2026-04-05 18:19:44,270 INFO 4579 rows -> crm.lnd_activity
# 2026-04-05 18:19:44,271 INFO Loading activity_types.csv
# 2026-04-05 18:19:44,577 INFO 4 rows -> crm.lnd_activity_types

# 2026-04-05 18:19:44,577 INFO Ingestion complete

# Process finished with exit code 0