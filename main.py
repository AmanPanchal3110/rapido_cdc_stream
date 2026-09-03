from fastapi import FastAPI
from backend.utils.db import Base, engine
from backend.users.router import user_router
from backend.riders.router import rider_router
from backend.drivers.router import driver_router

Base.metadata.create_all(engine)
app=FastAPI()

app.include_router(user_router)
app.include_router(rider_router)
app.include_router(driver_router)