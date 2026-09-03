from sqlalchemy.orm import Session
from backend.users.model import user_register_model
from backend.drivers.model import driver_data
from backend.drivers.dtos import updatedResponse

def profile(db:Session, user:user_register_model):
    is_driver=db.query(driver_data).filter(driver_data.driver_id == user.id).first()
    return {
        "user": user,
        "driver": is_driver
    }
    
def update_profile(data:updatedResponse,db:Session,user:user_register_model):
    is_driver=db.query(driver_data).filter(driver_data.driver_id == user.id).first()
    is_driver.vechicle_type = data.vechicle_type
    is_driver.vechicle_no = data.vechicle_no
    db.commit()
    db.refresh(is_driver)
    return is_driver