from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Text, UniqueConstraint

from config.database.session import Base


class AccountORM(Base):
    __tablename__ = "account2"

    id = Column(Integer, primary_key=True)
    email = Column(String(255))
    nickname = Column(String(255))
    bio = Column(Text)
    profile_image_url = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AccountInterestORM(Base):
    __tablename__ = "account_interest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, index=True)
    interest = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("account_id", "interest", name="uq_account_interest_unique"),
    )
