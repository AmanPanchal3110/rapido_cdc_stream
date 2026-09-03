from fastapi import Request, HTTPException, status, Depends
from sqlalchemy.orm import Session
import jwt
from backend.utils.setting import setting
from backend.users.model import user_register_model
from jwt.exceptions import InvalidTokenError
from backend.utils.db import get_db

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