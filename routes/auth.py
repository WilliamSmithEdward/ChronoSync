from fastapi import APIRouter
from datetime import datetime, timezone
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
        userName TEXT,
        created_at TEXT
    )
    """)

    c.commit()
    c.close()


def ensure_user_audit_table():

    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_audit(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audit_at TEXT NOT NULL,
        audit_by_user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        target_user_id INTEGER,
        token TEXT,
        is_admin INTEGER,
        created_at TEXT,
        FOREIGN KEY (audit_by_user_id) REFERENCES users(id)
    )
    """)

    c.commit()
    c.close()


@router.post("/register")
def register(token: str):

    ensure_user_table()
    ensure_user_audit_table()

    c = conn()
    cur = c.cursor()

    try:

        cur.execute("BEGIN IMMEDIATE")

        cur.execute(
            "SELECT id, is_admin, created_at FROM users WHERE token=?",
            (token,)
        )

        row = cur.fetchone()

        if row:
            c.commit()
            return {
                "id": row[0],
                "token": token,
                "is_admin": bool(row[1]),
                "roles": [],
                "existing": True
            }

        cur.execute("SELECT COUNT(*) FROM users")

        count = cur.fetchone()[0]

        is_admin = 1 if count == 0 else 0
        ts = datetime.now(timezone.utc).isoformat()

        cur.execute(
            "INSERT INTO users(token,is_admin,created_at) VALUES(?,?,?)",
            (token, is_admin, ts)
        )

        user_id = cur.lastrowid

        cur.execute(
            """
            INSERT INTO user_audit(
                audit_at,
                audit_by_user_id,
                action,
                target_user_id,
                token,
                is_admin,
                created_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                ts,
                user_id,
                "register",
                user_id,
                token,
                is_admin,
                ts
            )
        )

        c.commit()

    except Exception:
        c.rollback()
        raise

    finally:
        c.close()

    return {
        "id": user_id,
        "token": token,
        "is_admin": bool(is_admin),
        "roles": [],
        "existing": False
    }