from pydantic import BaseModel, field_validator
import re

class UserProfileResponse(BaseModel):
    id: str
    name: str
    email_id: str
    phn_no: str

    model_config = {
        "from_attributes": True
    }

class DriverProfileResponse(BaseModel):
    driver_id: str
    vechicle_type: str | None
    vechicle_no: str | None
    total_rides: int
    avg_rating: float
    is_active: bool

    model_config = {
        "from_attributes": True
    }


class ProfileResponse(BaseModel):
    user: UserProfileResponse
    driver: DriverProfileResponse
    
class updatedResponse(BaseModel):
    vechicle_type: str
    vechicle_no:str
    
    @field_validator("vechicle_type")
    @classmethod
    def valide_update(cls,value):
        if value.lower() not in {"bike","car","auto"}:
            raise ValueError("vehicle_type must be bike, auto, or car")
        return value.lower()
    
    @field_validator("vechicle_no")
    @classmethod
    def validate_no(cls,value):
        value=value.upper()
        if not re.fullmatch(r"[A-Z]{2}\d{2}[A-Z]{2}\d{4}",value):
            raise ValueError(
                "vehicle_no must be like HR12AB1234"
            )
        return value
