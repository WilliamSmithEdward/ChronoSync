from fastapi import APIRouter, HTTPException, Header
from db import conn

router = APIRouter()


def ensure_chronosync_dataset_table():

    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ChronoSyncDataset(
        table_name TEXT PRIMARY KEY,
        ChronoSyncDatasetName TEXT NOT NULL
    )
    """)

    c.commit()
    c.close()


@router.get("/datasets")
def list_datasets(
    x_chronosync_clientid: str | None = Header(default=None)
):

    if not x_chronosync_clientid:
        raise HTTPException(400, "X-ChronoSync-ClientId header required")

    ensure_chronosync_dataset_table()

    c = conn()
    cur = c.cursor()

    try:

        cur.execute(
            "SELECT id, is_admin FROM users WHERE token=?",
            (x_chronosync_clientid,)
        )

        auth_row = cur.fetchone()

        if auth_row is None:
            raise HTTPException(401, "invalid auth")

        cur.execute("""
        SELECT m.name, COALESCE(d.ChronoSyncDatasetName, '')
        FROM sqlite_master m
        LEFT JOIN ChronoSyncDataset d
            ON d.table_name = m.name
        WHERE m.type='table'
          AND m.name GLOB '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
        ORDER BY m.name ASC
        """)

        rows = cur.fetchall()

    finally:
        c.close()

    return {
        "datasets": [
            {
                "table_name": row[0],
                "ChronoSyncDatasetName": row[1]
            }
            for row in rows
        ]
    }

@router.post("/datasets/set")
def set_dataset(
    datasets: list[dict],
    x_chronosync_clientid: str | None = Header(default=None)
):

    if not x_chronosync_clientid:
        raise HTTPException(400, "X-ChronoSync-ClientId header required")

    ensure_chronosync_dataset_table()

    c = conn()
    cur = c.cursor()

    try:

        cur.execute("BEGIN IMMEDIATE")

        cur.execute(
            "SELECT id, is_admin FROM users WHERE token=?",
            (x_chronosync_clientid,)
        )

        auth_row = cur.fetchone()

        if auth_row is None:
            raise HTTPException(401, "invalid auth")

        if int(auth_row[1]) != 1:
            raise HTTPException(403, "admin required")

        normalized_items = []

        for item in datasets:

            if not isinstance(item, dict):
                raise HTTPException(400, "each dataset must be an object")

            if "table_name" not in item:
                raise HTTPException(400, "table_name required")

            table_name = item["table_name"]
            chrono_sync_dataset_name = item.get("ChronoSyncDatasetName", "")

            if not isinstance(table_name, str):
                raise HTTPException(400, "valid table_name required")

            table_name = table_name.strip()

            if len(table_name) != 32:
                raise HTTPException(400, "valid table_name required")

            if table_name != table_name.lower():
                raise HTTPException(400, "valid table_name required")

            if any(ch not in "0123456789abcdef" for ch in table_name):
                raise HTTPException(400, "valid table_name required")

            if not isinstance(chrono_sync_dataset_name, str):
                raise HTTPException(400, "ChronoSyncDatasetName must be a string")

            chrono_sync_dataset_name = chrono_sync_dataset_name.strip()

            normalized_items.append(
                {
                    "table_name": table_name,
                    "ChronoSyncDatasetName": chrono_sync_dataset_name
                }
            )

        seen_table_names = set()

        for item in normalized_items:

            if item["table_name"] in seen_table_names:
                raise HTTPException(400, "duplicate table_name in datasets array")

            seen_table_names.add(item["table_name"])

            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (item["table_name"],)
            )

            if cur.fetchone() is None:
                raise HTTPException(404, f"table_name not found: {item['table_name']}")

        cur.execute("DELETE FROM ChronoSyncDataset")

        cur.executemany(
            """
            INSERT INTO ChronoSyncDataset(
                table_name,
                ChronoSyncDatasetName
            ) VALUES(?,?)
            """,
            [
                (
                    item["table_name"],
                    item["ChronoSyncDatasetName"]
                )
                for item in normalized_items
            ]
        )

        c.commit()

    except Exception:
        c.rollback()
        raise

    finally:
        c.close()

    return {
        "ok": True
    }