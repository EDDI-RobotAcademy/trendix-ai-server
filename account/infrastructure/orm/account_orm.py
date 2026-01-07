from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Text, UniqueConstraint, JSON

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


class AccountDashboardLayoutORM(Base):
    """계정별 대시보드 레이아웃 저장"""
    __tablename__ = "account_dashboard_layout"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, unique=True, index=True, nullable=False)
    widgets = Column(JSON, nullable=False)  # 위젯 목록 (JSON)
    layouts = Column(JSON, nullable=False)  # 레이아웃 정보 (JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
