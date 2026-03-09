import sqlite3

DB = "chronosync.db"

def conn():
    return sqlite3.connect(DB)

def ensure_registry():

    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS table_registry(
        logical_name TEXT PRIMARY KEY,
        physical_name TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    c.commit()
    c.close()