from fastapi import APIRouter, Depends, status
from backend.users import controller
from backend.utils.db import get_db
from backend.users.dtos import user_schema, response_user_schema, login_schema
user_router=APIRouter(prefix="/user")

@user_router.post("/create",response_model=response_user_schema,status_code=status.HTTP_201_CREATED)
def user_register(body:user_schema,db=Depends(get_db)):
    return controller.user_register(body,db)

@user_router.post("/login",status_code=status.HTTP_202_ACCEPTED)
def user_login(body:login_schema,db=Depends(get_db)):
    return controller.user_login(body,db)