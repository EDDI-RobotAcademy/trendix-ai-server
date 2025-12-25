from typing import Set
import psycopg2
from psycopg2.extras import RealDictCursor

from config.database.session import SessionLocal
from content.infrastructure.orm.models import (
    StopwordORM
)

class StopwordRepositoryImpl():
    
    # 클래스 변수로 선언
    __instance = None

    def __init__(self):
        self.db = SessionLocal()

    @classmethod
    def getInstance(cls):
        if cls.__instance is None:
            cls.__instance = cls()
        return cls.__instance

    def get_stopwords(self, lang: str = "ko") -> Set[str]:
        try:
            query = (
                self.db.query(StopwordORM.word)
                .filter(
                    StopwordORM.lang == lang,
                    StopwordORM.enabled == True
                )
            )
            rows = query.all()
            return {row.word for row in rows}
        finally:
            self.db.close()
        