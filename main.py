from fastapi import FastAPI
from backend.utils.db import Base, engine
from backend.users.router import user_router
Base.metadata.create_all(engine)
app=FastAPI()

app.include_router(user_router)