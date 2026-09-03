from sqlalchemy import *
from backend.utils.db import Base

class user_register_model(Base):
    __tablename__="user_data"
    
    id=Column(String, primary_key=True)
    name=Column(String, nullable=False)
    email_id=Column(String, nullable=False)
    hash_password=Column(String, nullable=False)
    phn_no=Column(String, nullable=False)
    role=Column(String, nullable=False)
    