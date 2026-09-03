from fastapi import APIRouter, Depends, status
from backend.utils.db import get_db
from backend.users.model import user_register_model
from backend.utils.helpher import driver_auth
from backend.drivers import controller
from backend.drivers.dtos import ProfileResponse, updatedResponse, DriverProfileResponse


driver_router=APIRouter(prefix="/driver")

@driver_router.get("/profile",status_code=status.HTTP_202_ACCEPTED,response_model=ProfileResponse)
def profile(db = Depends(get_db),user:user_register_model = Depends(driver_auth)):
    return controller.profile(db,user)

@driver_router.put("/update",status_code=status.HTTP_201_CREATED,response_model=DriverProfileResponse)
def update_profile(data:updatedResponse, db = Depends(get_db), user:user_register_model = Depends(driver_auth)):
    return controller.update_profile(data,db,user)