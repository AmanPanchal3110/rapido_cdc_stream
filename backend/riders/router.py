from fastapi import APIRouter, Depends, status
from backend.utils.db import get_db
from backend.users.model import user_register_model
from backend.utils.helpher import rider_auth
from backend.riders import controller
from backend.riders.dtos import ProfileResponse, updatedProfile, RiderProfileResponse

rider_router=APIRouter(prefix="/rider")

@rider_router.get("/profile",status_code=status.HTTP_200_OK,response_model=ProfileResponse)
def get_profile(db= Depends(get_db),user:user_register_model = Depends(rider_auth)):
    return controller.view_profile(db,user)

@rider_router.put("/update",status_code=status.HTTP_201_CREATED,response_model=RiderProfileResponse)
def update_profile(data: updatedProfile, db = Depends(get_db), user:user_register_model = Depends(rider_auth)):
    return controller.updatedProfile(data,db,user)