from fastapi import APIRouter
from datetime import datetime
from db import conn

router = APIRouter()


def ensure_user_table():

    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE,
        is_admin INTEGER,
        created_at TEXT
    )
    """)

    c.commit()
    c.close()


@router.post("/register")
def register(token: str):

    ensure_user_table()

    c = conn()
    cur = c.cursor()

    # check if token already exists
    cur.execute(
        "SELECT id, is_admin FROM users WHERE token=?",
        (token,)
    )

    row = cur.fetchone()

    if row:
        c.close()
        return {
            "id": row[0],
            "token": token,
            "is_admin": bool(row[1]),
            "existing": True
        }

    # check if first user
    cur.execute("SELECT COUNT(*) FROM users")

    count = cur.fetchone()[0]

    is_admin = 1 if count == 0 else 0

    ts = datetime.utcnow().isoformat()

    cur.execute(
        "INSERT INTO users(token,is_admin,created_at) VALUES(?,?,?)",
        (token, is_admin, ts)
    )

    user_id = cur.lastrowid

    c.commit()
    c.close()

    return {
        "id": user_id,
        "token": token,
        "is_admin": bool(is_admin),
        "existing": False
    }