from sqlalchemy.orm import Session
from backend.users.model import user_register_model
from backend.riders.model import rider_data
    
def view_profile(db:Session,user:user_register_model):
    is_rider=db.query(rider_data).filter(rider_data.rider_id == user.id).first()
    return {
        "user": user,
        "rider": is_rider
    }