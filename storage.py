import re
import uuid
from datetime import datetime
from db import conn

def sanitize(name):
    return re.sub(r'[^a-z0-9_]', '_', name.lower())


def get_or_create_table(name):

    name = sanitize(name)

    c = conn()
    cur = c.cursor()

    cur.execute(
        "SELECT physical_name FROM table_registry WHERE logical_name=?",
        (name,)
    )

    row = cur.fetchone()

    if row:
        c.close()
        return row[0]

    guid = uuid.uuid4().hex
    physical = f"{name}_{guid}"

    cur.execute(f"""
    CREATE TABLE {physical}(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        payload TEXT
    )
    """)

    cur.execute(
        "INSERT INTO table_registry VALUES (?, ?, ?)",
        (name, physical, datetime.utcnow().isoformat())
    )

    c.commit()
    c.close()

    return physical