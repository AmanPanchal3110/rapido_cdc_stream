from pydantic import BaseModel

class UserProfileResponse(BaseModel):
    id: str
    name: str
    email_id: str
    phn_no: str

    model_config = {
        "from_attributes": True
    }

class RiderProfileResponse(BaseModel):
    rider_id: str
    city: str | None
    total_rides: int
    avg_rating: float

    model_config = {
        "from_attributes": True
    }


class ProfileResponse(BaseModel):
    user: UserProfileResponse
    rider: RiderProfileResponse
    
class updatedProfile(BaseModel):
    city: str