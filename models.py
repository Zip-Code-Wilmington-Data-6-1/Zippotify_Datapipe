from sqlalchemy import Column, Integer, String, BigInteger, TIMESTAMP
from .database import Base

class DimUser(Base):
    __tablename__ = "dim_user"
    user_id = Column(Integer, primary_key=True, index=True)
    gender = Column(String(10))
    registration_ts = Column(TIMESTAMP)
    birthday = Column(BigInteger)