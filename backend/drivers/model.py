from sqlalchemy import *
from backend.utils.db import Base

class driver_data(Base):
    __tablename__="drivers"
    
    driver_id=Column(String, ForeignKey("user_data.id",ondelete="CASCADE"),primary_key=True)
    vechicle_type=Column(String,nullable=True)
    vechicle_no=Column(String,nullable=True)
    total_rides=Column(INTEGER,default=0)
    avg_rating=Column(Numeric(3, 1),default=0.0)
    is_active=Column(Boolean,default=True)
    created_at=Column(DateTime,server_default=func.now())
    updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())