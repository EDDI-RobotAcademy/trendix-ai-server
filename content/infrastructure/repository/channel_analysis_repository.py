from datetime import datetime, timedelta

from sqlalchemy import text

from config.database.session import SessionLocal


class ChannelAnalysisRepository:
    def __init__(self):
        self.db = SessionLocal()

    def fetch_category_average_views(
        self,
        category_id: int | None,
        platform: str | None,
        days: int,
    ) -> float | None:
        # 한국어 주석: 카테고리별 최근 N일 평균 조회수를 비교 기준으로 사용한다.
        if category_id is None:
            return None

        since_date = datetime.utcnow() - timedelta(days=days)
        row = self.db.execute(
            text(
                """
                SELECT AVG(view_count) AS avg_view
                FROM video
                WHERE category_id = :category_id
                  AND (:platform IS NULL OR platform = :platform)
                  AND view_count IS NOT NULL
                  AND COALESCE(published_at, crawled_at) >= :since_date
                """
            ),
            {
                "category_id": category_id,
                "platform": platform,
                "since_date": since_date,
            },
        ).mappings().first()

        if not row or row["avg_view"] is None:
            return None
        return float(row["avg_view"])
