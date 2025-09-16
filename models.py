from sqlalchemy import Column, Integer, String, BigInteger, TIMESTAMP, DECIMAL
from database import Base

class DimUser(Base):
    __tablename__ = "dim_user"
    user_id = Column(Integer, primary_key=True, index=True)
    last_name = Column(String(100))
    first_name = Column(String(100))
    gender = Column(String(10))
    registration_ts = Column(TIMESTAMP)
    birthday = Column(BigInteger)

class DimLocation(Base):
    __tablename__ = "dim_location"
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    state = Column(String(50))
    latitude = Column(DECIMAL(9, 6))
    longitude = Column(DECIMAL(9, 6))