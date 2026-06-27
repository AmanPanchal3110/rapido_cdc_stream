from faker import Faker
import uuid
from psycopg2 import pool
import threading
from datetime import datetime
import random
import time

fake=Faker('en_IN')

conn_pool=pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=25,
    host="localhost",
    port=5432,
    dbname="rapido_db",
    user="Aman",
    password="panchal@3110"
)
conn=conn_pool.getconn()
cur=conn.cursor()

cur.execute("""
            CREATE TABLE IF NOT EXISTS rides(
                ride_id       VARCHAR(12) PRIMARY KEY,   
                rider_id      VARCHAR(10) REFERENCES riders(rider_id),
                driver_id     VARCHAR(10) REFERENCES drivers(driver_id), 
                status        VARCHAR(20) DEFAULT 'requested', 
                pickup        VARCHAR(100),              
                drop_loc      VARCHAR(100),              
                vehicle_type  VARCHAR(10),               
                fare          NUMERIC(6,2),              
                distance_km   NUMERIC(5,2),              
                rider_rating  NUMERIC(2,1),              
                driver_rating NUMERIC(2,1),              
                created_at    TIMESTAMP DEFAULT NOW(),   
                updated_at    TIMESTAMP DEFAULT NOW());""")

cur.execute("""ALTER TABLE drivers ADD COLUMN IF NOT EXISTS is_busy BOOLEAN DEFAULT FALSE;""")
cur.execute("""ALTER TABLE riders ADD COLUMN IF NOT EXISTS is_riding BOOLEAN DEFAULT FALSE;""")

conn.commit()
cur.close()
conn_pool.putconn(conn)

print("data loaded")

conn = conn_pool.getconn()
cur  = conn.cursor()
cur.execute("UPDATE drivers SET is_busy = FALSE;")
cur.execute("UPDATE riders SET is_riding = FALSE;")
cur.execute("""
    UPDATE rides 
    SET status = 'cancelled', updated_at = %s
    WHERE status NOT IN ('completed', 'cancelled');
""", (datetime.now(),))
conn.commit()
cur.close()
conn_pool.putconn(conn)
print("Cleanup done!")
location=[
    "fazilpur", "Rohini", "Dwarka", "Lajpat Nagar",
    "Saket", "Karol Bagh", "Vasant Kunj", "Janakpuri",
    "Noida Sector 18", "Gurugram Cyber City", "Faridabad"
]
status_flow=["confirmed","driver_reached","picked_up","on_destination","completed"]

def ride_flow(driver_id,vehicle_type,rider_id):
    conn=conn_pool.getconn()
    cur=conn.cursor()
    try:
        ride_id="RD"+str(uuid.uuid4())[:8].upper()
        pickup=random.choice(location)
        drop_loc=random.choice([l for l in location if l!=pickup])
        dist=round(random.uniform(1.5,25.0),2)
        fare=round(dist*random.choice([8,10,12]),2)
        
        cur.execute("""
                    INSERT INTO rides(ride_id,rider_id,driver_id,status,pickup,drop_loc,vehicle_type,fare,distance_km,rider_rating,driver_rating,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
                    (
                        ride_id,rider_id,driver_id,"requested",pickup,drop_loc,vehicle_type
                        ,fare,dist,None,None,datetime.now(),datetime.now()
                    ))
        conn.commit()
        print("ride_id is inserted")
        
        if random.random()<0.2:
            time.sleep(random.uniform(0.5,1.5))
            cur.execute("""UPDATE rides
                        SET status='cancelled',updated_at=%s
                        WHERE ride_id=%s;
                        """,(datetime.now(),ride_id))
            conn.commit()
            print("status updated cancelled")
            return
        
        for status in status_flow:
            time.sleep(random.uniform(1,3))
            if status=="completed":
                rider_rating=round(random.uniform(2.0,5.0),1)
                driver_rating=round(random.uniform(2.0,5.0),1)
                cur.execute("""UPDATE rides
                            SET status=%s,rider_rating=%s,driver_rating=%s,updated_at=%s WHERE ride_id=%s;""",
                            (status,rider_rating,driver_rating,datetime.now(),ride_id))
                cur.execute("""UPDATE drivers
                            SET total_rides = total_rides + 1,
                            avg_rating  = (avg_rating * total_rides + %s) / (total_rides + 1)
                            WHERE driver_id = %s;""",(driver_rating,driver_id))
                cur.execute("""UPDATE riders
                            SET total_rides=total_rides+1,
                            avg_rating=(avg_rating * total_rides + %s) / (total_rides + 1)
                            WHERE rider_id=%s;""",(rider_rating,rider_id))
            else:
                cur.execute("""UPDATE rides
                            SET status=%s,updated_at=%s WHERE ride_id=%s;""",
                            (status,datetime.now(),ride_id))
            conn.commit()
            print("status updated")
    except Exception as e:
        print(f"error in ride flow:{e}")
        conn.rollback()
    finally:
        try:
            cur.execute("""UPDATE drivers SET is_busy=FALSE WHERE driver_id=%s;""",(driver_id,))
            cur.execute("""UPDATE riders SET is_riding=FALSE WHERE rider_id=%s;""",(rider_id,))
            conn.commit()
            print(f"FREE | Driver: {driver_id} | Rider: {rider_id}")
        except Exception as e:
            print(f"Free karne mein error: {e}")
        finally:
            cur.close()
            conn_pool.putconn(conn)

print("simulation start")
MAX_THREAD=20
driver_lock=threading.Lock()
while True:
    active=threading.active_count()-1
    if active<MAX_THREAD:
        with driver_lock:
            conn=conn_pool.getconn()
            cur=conn.cursor()
            cur.execute("""SELECT driver_id,vehicle_type FROM drivers
                WHERE is_active=TRUE AND is_busy=FALSE;""")
            free_driver=cur.fetchall()
            cur.execute("""
                SELECT rider_id FROM riders WHERE is_riding=FALSE;
            """)
            free_rider=[row[0] for row in cur.fetchall()]
            
            if not free_driver or not free_rider:
                cur.close()
                conn_pool.putconn(conn)
                print("sab busy hai")
                time.sleep(2)
                continue
            driver_id,vehicle_type=random.choice(free_driver)
            rider_id=random.choice(free_rider)
            cur.execute("""UPDATE drivers SET is_busy=TRUE WHERE driver_id=%s;""",(driver_id,))
            cur.execute("UPDATE riders SET is_riding=TRUE WHERE rider_id=%s;", (rider_id,))
            conn.commit()
            cur.close()
            conn_pool.putconn(conn)
        t=threading.Thread(target=ride_flow,args=(driver_id,vehicle_type,rider_id))
        t.daemon = True
        t.start()
        time.sleep(random.uniform(0.1,0.5))
    else:
        time.sleep(1)