import json
from fastapi import APIRouter, HTTPException
from db import conn

router = APIRouter()

@router.get("/latest-id")
def latest_id(table_id: str):

    c = conn()
    cur = c.cursor()

    cur.execute(f'SELECT COALESCE(MAX(id),0) FROM "{table_id}" ORDER BY id DESC LIMIT 1')
    result = cur.fetchone()[0]

    c.close()

    return {"latest_id": result}


@router.get("/latest")
def latest(table_id: str):

    c = conn()
    cur = c.cursor()

    cur.execute(
        f'SELECT id, created_at, payload FROM "{table_id}" ORDER BY id DESC LIMIT 1'
    )

    r = cur.fetchone()
    c.close()

    if not r:
        raise HTTPException(404, "no data")

    return {
        "id": r[0],
        "timestamp": r[1],
        "payload": json.loads(r[2])
    }