from sqlalchemy.orm import Session
from backend.users.model import user_register_model
from backend.drivers.model import driver_data

def profile(db:Session, user:user_register_model):
    is_driver=db.query(driver_data).filter(driver_data.driver_id == user.id).first()
    return {
        "user": user,
        "driver": is_driver
    }