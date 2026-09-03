from sqlalchemy.orm import Session
from backend.users.dtos import user_schema, login_schema
from backend.users.model import user_register_model
from fastapi import HTTPException, status
import uuid
from pwdlib import PasswordHash
import jwt
from datetime import datetime, timedelta
from backend.utils.setting import setting

password_hash = PasswordHash.recommended()
def get_password(password):
    return password_hash.hash(password)

#verify password
def verify_password(plain_password,hashed_password):
    return password_hash.verify(plain_password,hashed_password)

#generate_userid
def generate_id(db:Session,role:str):
    if role not in ("rider", "driver"):
        raise HTTPException(
            status_code=400,
            detail="Invalid role"
        )
    prefix="R" if role == "rider" else "D"
    while True:
        user_id = f"{prefix}/{uuid.uuid4().hex[:8].upper()}"
        is_user=db.query(user_register_model).filter(user_register_model.id == user_id).first()
        if is_user is None:
            return user_id

def user_register(body:user_schema,db:Session):
    is_user=db.query(user_register_model).filter(user_register_model.email_id == body.email).first()
    if is_user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="mail id already exist")
    
    is_user=db.query(user_register_model).filter(user_register_model.phn_no == body.phn_no).first()
    if is_user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="phone no already exist")
    
    #user_id
    user_id=generate_id(db,body.role)
    
    #password hash
    hash_password_=get_password(body.password)
    
    #add user
    new_user=user_register_model(id=user_id
                                 ,name=body.name
                                 ,hash_password=hash_password_
                                 ,email_id=body.email
                                 ,phn_no=body.phn_no
                                 ,role=body.role)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    return new_user

def user_login(body:login_schema,db:Session):
    if body.login_type == "email":
        is_user=db.query(user_register_model).filter(user_register_model.email_id == body.login).first()
    else:
        is_user=db.query(user_register_model).filter(user_register_model.phn_no == body.login).first()
    if not is_user:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="invalid login")
    if not verify_password(body.password,is_user.hash_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,detail="wrong password")
    #create token
    exp_time=datetime.now()+timedelta(seconds=setting.EXP_TIME)
    token=jwt.encode({"_id":is_user.id,"exp":exp_time,"role":is_user.role},setting.SECRET_KEY,setting.ALGORITHM)
    return {"token": token}
    
    
    