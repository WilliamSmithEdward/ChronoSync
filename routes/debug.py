import os
from fastapi import APIRouter, HTTPException
from db import conn

router = APIRouter()

@router.get("/debug-dataset")
def debug_dataset(table_id: str):

    if not table_id:
        raise HTTPException(400, "table_id required")

    physical = f'"{table_id}"'

    c = conn()
    cur = c.cursor()

    # verify table exists
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_id,)
    )

    exists = cur.fetchone()

    if not exists:
        c.close()
        raise HTTPException(404, "dataset not found")

    # get all row ids
    cur.execute(f"SELECT id, created_at FROM {physical} ORDER BY id")
    rows = cur.fetchall()

    # get latest id via MAX
    cur.execute(f"SELECT COALESCE(MAX(id),0) FROM {physical}")
    max_id = cur.fetchone()[0]

    # check sqlite sequence counter
    cur.execute(
        "SELECT seq FROM sqlite_sequence WHERE name=?",
        (table_id,)
    )
    seq = cur.fetchone()

    c.close()

    return {
        "table_id": table_id,
        "physical_table": table_id,
        "database_file": os.path.abspath("chronostore.db"),
        "row_count": len(rows),
        "rows": rows,
        "max_id": max_id,
        "sqlite_sequence": seq[0] if seq else None
    }