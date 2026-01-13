import os
import urllib.parse

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

load_dotenv()

# Uses SQL_* env vars provided (e.g., Supabase): SQL_USER, SQL_PASSWORD, SQL_HOST, SQL_PORT, SQL_DATABASE
# 한국어 주석: Supabase 등에서 제공한 PostgreSQL 접속 정보를 사용하여 SQLAlchemy 엔진을 만듭니다.
password = urllib.parse.quote_plus(os.getenv("SQL_PASSWORD", ""))

# Synchronous database URL
DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('SQL_USER','postgres')}:{password}"
    f"@{os.getenv('SQL_HOST','localhost')}:{os.getenv('SQL_PORT','5432')}/{os.getenv('SQL_DATABASE','apple_mango')}"
)

# Asynchronous database URL (using asyncpg driver)
ASYNC_DATABASE_URL = (
    f"postgresql+asyncpg://{os.getenv('SQL_USER','postgres')}:{password}"
    f"@{os.getenv('SQL_HOST','localhost')}:{os.getenv('SQL_PORT','5432')}/{os.getenv('SQL_DATABASE','apple_mango')}"
)

# Synchronous engine and session
engine = create_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    pool_pre_ping=True,
    pool_recycle= 300,  # 5분마다 재사용
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Asynchronous engine and session
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={
        "statement_cache_size": 0
    }
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

Base = declarative_base()


def get_db_session():
    return SessionLocal()


def init_db_schema():
    """
    애플리케이션 기동 시 테이블이 없을 경우를 대비해 스키마를 생성합니다.
    """
    Base.metadata.create_all(bind=engine)
