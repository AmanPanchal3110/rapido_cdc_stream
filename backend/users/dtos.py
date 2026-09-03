from pydantic import BaseModel, field_validator
from typing import Literal

class user_schema(BaseModel):
    name: str
    email: str
    phn_no: str
    password: str
    role: str

    @field_validator("phn_no")
    @classmethod
    def validate_phone(cls, value):
        if not value.isdigit() or len(value) != 10 or value[0] not in "6789":
            raise ValueError("invalid phn_no")
        return value
    
class response_user_schema(BaseModel):
    id:str
    name:str
    email_id:str
    phn_no:str
    role:str
    
class login_schema(BaseModel):
    login_type:Literal["email","phn_no"]
    login:str
    password:str