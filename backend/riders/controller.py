from sqlalchemy.orm import Session
from backend.users.model import user_register_model
from backend.riders.model import rider_data
from backend.riders.dtos import updatedProfile
    
def view_profile(db:Session,user:user_register_model):
    is_rider=db.query(rider_data).filter(rider_data.rider_id == user.id).first()
    return {
        "user": user,
        "rider": is_rider
    }
    
def updatedProfile(data:updatedProfile ,db:Session ,user:user_register_model):
    is_rider=db.query(rider_data).filter(rider_data.rider_id == user.id).first()
    is_rider.city = data.city
    db.commit()
    db.refresh(is_rider)
    return is_rider