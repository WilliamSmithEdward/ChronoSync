from fastapi import APIRouter, HTTPException, Header
from datetime import datetime, timezone
from db import conn

router = APIRouter()


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


@router.get("/users")
def list_users():

    c = conn()
    cur = c.cursor()

    cur.execute("""
    SELECT id, token, userName, is_admin, created_at
    FROM users
    ORDER BY id ASC
    """)

    rows = cur.fetchall()

    c.close()

    return {
        "users": [
            {
                "id": row[0],
                "token": row[1],
                "userName": row[2],
                "is_admin": bool(row[3]),
                "created_at": row[4]
            }
            for row in rows
        ]
    }


@router.post("/users/set")
def set_users(
    users: list[dict],
    x_chronosync_clientid: str | None = Header(default=None)
):

    if not x_chronosync_clientid:
        raise HTTPException(400, "X-ChronoSync-ClientId header required")

    ensure_user_audit_table()

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

        audit_by_user_id = int(auth_row[0])
        ts = datetime.now(timezone.utc).isoformat()

        normalized_users = []

        for item in users:

            if not isinstance(item, dict):
                raise HTTPException(400, "each user must be an object")

            if "id" not in item:
                raise HTTPException(400, "id required")
            if "userName" not in item:
                raise HTTPException(400, "userName required")
            if "is_admin" not in item:
                raise HTTPException(400, "is_admin required")

            user_id = item["id"]
            user_name = item["userName"]
            is_admin = item["is_admin"]

            if not isinstance(user_id, int):
                raise HTTPException(400, "id must be an integer")

            if not isinstance(user_name, str) or not user_name.strip():
                raise HTTPException(400, "userName must be a non-empty string")

            if isinstance(is_admin, bool):
                is_admin_value = 1 if is_admin else 0
            elif isinstance(is_admin, int) and is_admin in (0, 1):
                is_admin_value = is_admin
            else:
                raise HTTPException(400, "is_admin must be a boolean or 0/1")

            normalized_users.append(
                {
                    "id": user_id,
                    "userName": user_name.strip(),
                    "is_admin": is_admin_value
                }
            )

        seen_ids = set()

        for item in normalized_users:

            if item["id"] in seen_ids:
                raise HTTPException(400, "duplicate id in users array")

            seen_ids.add(item["id"])

        cur.execute("""
        SELECT id, userName, token, is_admin, created_at
        FROM users
        ORDER BY id ASC
        """)

        existing_rows = cur.fetchall()

        existing_by_id = {
            int(row[0]): {
                "id": int(row[0]),
                "userName": row[1],
                "token": row[2],
                "is_admin": int(row[3]),
                "created_at": row[4]
            }
            for row in existing_rows
        }

        for item in normalized_users:

            existing = existing_by_id.get(item["id"])

            if existing is None:
                raise HTTPException(400, f"user id {item['id']} not found")

            if existing["userName"] == item["userName"] and existing["is_admin"] == item["is_admin"]:
                continue

            cur.execute(
                """
                UPDATE users
                SET userName=?, is_admin=?
                WHERE id=?
                """,
                (
                    item["userName"],
                    item["is_admin"],
                    item["id"]
                )
            )

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
                    audit_by_user_id,
                    "update",
                    item["id"],
                    existing["token"],
                    item["is_admin"],
                    existing["created_at"]
                )
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