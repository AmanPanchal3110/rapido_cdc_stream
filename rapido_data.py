from faker import Faker
import pandas as pd
import random
from datetime import datetime
import uuid

fake=Faker('en_IN')

def data():
    driver_data=[]
    for i in range(20):
        driver_data.append({
            "driver_id": f"D:{str(uuid.uuid4())[:6]}",
            "driver_name":fake.name(),
            "vehicle_type":random.choice(["bike","car","auto"]),
            "vehicle_no": f"{random.choice(['DL','HR','RJ','GJ','MH'])} {random.randint(10,99):02d} {''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))} {random.randint(1000,9999)}",
            "total_rides":random.randint(0,500),
            "avg_rating": round(random.uniform(2,5),1),
            "created_at": datetime.now().isoformat()
        })
    rider_data=[]    
    for i in range(200):
        name=fake.name()
        email_id=name.lower().replace(" ", ".")
        rider_data.append({
            "rider_id": f"R:{str(uuid.uuid4())[:6]}",
            "rider_name": name,
            "phn_no": f"+91-{random.choice(['6','7','8','9'])}{random.randint(100000000, 999999999)}",
            "email_id": email_id,
            "city":fake.city(),
            "created_at": datetime.now().isoformat()
        })
    rdriver_data = pd.DataFrame(driver_data)
    rrider_data = pd.DataFrame(rider_data)  
    
    return rdriver_data,rrider_data

d_data,r_data=data()

print(d_data)
print("/n")
print(r_data)
    
        