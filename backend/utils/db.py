from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from backend.utils.setting import setting


Base=declarative_base()


engine=create_engine(url=setting.DB_CONNECTION)

local_session=sessionmaker(engine)

def get_db():
    session=local_session()
    try:
        yield session
    finally:
        session.close()