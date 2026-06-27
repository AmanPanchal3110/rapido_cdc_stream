from faker import Faker
import random
from datetime import datetime
import uuid
import psycopg2

conn= psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="rapido_db",
    user="Aman",
    password="panchal@3110"
)
cur=conn.cursor()

cur.execute("""
            CREATE TABLE IF NOT EXISTS drivers(
            driver_id     VARCHAR(10) PRIMARY KEY,
            driver_name   VARCHAR(100),
            vehicle_type  VARCHAR(10),
            vehicle_no    VARCHAR(20),
            total_rides   INT DEFAULT 0,
            avg_rating    NUMERIC(3,1) DEFAULT 0.0,
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMP DEFAULT NOW());""")
cur.execute("""
            CREATE TABLE IF NOT EXISTS riders(
            rider_id    VARCHAR(10) PRIMARY KEY,
            rider_name  VARCHAR(100),
            phone       VARCHAR(15),
            email       VARCHAR(100),
            city        VARCHAR(50),
            total_rides   INT DEFAULT 0,
            avg_rating    NUMERIC(3,1) DEFAULT 0.0,
            created_at  TIMESTAMP DEFAULT NOW());""")

conn.commit()

fake=Faker('en_IN')

def drivers_data(n):
    driver_data=[]
    for i in range(n):
        driver_data.append((
            "D"+str(uuid.uuid4())[:6].upper(),
            fake.name(),
            random.choice(["bike","car","auto"]),
            f"{random.choice(['DL','HR','RJ','GJ','MH'])} {random.randint(10,99):02d} {''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))} {random.randint(1000,9999)}",
            0,
            0.0,
            True,
            datetime.now()
        ))
    return driver_data
    
def rider_data(n):        
    rider_data=[]    
    for i in range(n):
        name=fake.name()
        email_id=name.lower().replace(" ", ".")+f"{random.randint(1,99)}@gmail.com"
        rider_data.append((
            "R"+str(uuid.uuid4())[:6].upper(),
            name,
            f"+91-{random.choice(['6','7','8','9'])}{random.randint(100000000, 999999999)}",
            email_id,
            fake.city(),
            0,
            0.0,
            datetime.now()
        ))
    return rider_data

drivers=drivers_data(50)
riders=rider_data(200)

cur.executemany("""
                INSERT INTO drivers(driver_id,driver_name,vehicle_type,vehicle_no,total_rides,avg_rating,is_active,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (driver_id) DO NOTHING;""",drivers)
cur.executemany("""
                INSERT INTO riders(rider_id,rider_name,phone,email,city,total_rides,avg_rating,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (rider_id) DO NOTHING;""",riders)

conn.commit()
print("data created")
cur.close()
conn.close()
    
        