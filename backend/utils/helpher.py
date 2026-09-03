from fastapi import Request, HTTPException, status, Depends
from sqlalchemy.orm import Session
import jwt
from backend.utils.setting import setting
from backend.users.model import user_register_model
from jwt.exceptions import InvalidTokenError
from backend.utils.db import get_db
from backend.riders.model import rider_data
from backend.drivers.model import driver_data

def is_auth(request:Request,db:Session = Depends(get_db)):
    try:
        token=request.headers.get("authorization")
        if not token:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="unauthorize")
        token=token.split(" ")[-1]
        token=jwt.decode(token,setting.SECRET_KEY,setting.ALGORITHM)
        is_user=db.query(user_register_model).filter(user_register_model.id == token.get("_id")).first()
        if not is_user:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="unauthorize")
        return is_user
    except InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="unauthorize")
    
def rider_auth(user:rider_data = Depends(is_auth)):
    if user.role != "rider":
        raise HTTPException(status.HTTP_406_NOT_ACCEPTABLE,detail="rider not present")
    return user

def driver_auth(user:driver_data = Depends(is_auth)):
    if user.role != "driver":
       raise HTTPException(status.HTTP_406_NOT_ACCEPTABLE,detail="driver not present")
    return user 
