import json
import uuid
import re
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from db import conn

router = APIRouter()

TABLE_ID_RE = re.compile(r"^[a-f0-9]{32}$")

@router.post("/write")
def write(body: dict):

    payload = body.get("payload")
    table_id = body.get("table_id")

    if payload is None:
        raise HTTPException(400, "payload required")

    creating = False

    if not table_id:
        table_id = uuid.uuid4().hex
        creating = True
    else:
        if not TABLE_ID_RE.match(table_id):
            raise HTTPException(400, "invalid table_id")

    table = f'"{table_id}"'
    ts = datetime.now(timezone.utc).isoformat()

    c = conn()
    cur = c.cursor()

    try:

        cur.execute("BEGIN IMMEDIATE")

        if creating:

            cur.execute(f"""
            CREATE TABLE {table}(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """)

        else:

            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_id,)
            )

            if cur.fetchone() is None:
                raise HTTPException(404, "table_id not found")

        cur.execute(
            f'INSERT INTO {table}(created_at,payload) VALUES (?,?)',
            (ts, json.dumps(payload))
        )

        new_id = cur.lastrowid

        c.commit()

    except Exception:
        c.rollback()
        raise

    finally:
        c.close()

    return {
        "table_id": table_id,
        "id": new_id,
        "timestamp": ts
    }