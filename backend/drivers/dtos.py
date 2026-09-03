from pydantic import BaseModel

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