from fastapi import FastAPI
from routes.auth import router as auth_router
from routes.debug import router as debug_router
from routes.write import router as write_router
from routes.query import router as query_router
from db import ensure_registry

app = FastAPI()

ensure_registry()

app.include_router(auth_router)
app.include_router(debug_router)
app.include_router(write_router)
app.include_router(query_router)