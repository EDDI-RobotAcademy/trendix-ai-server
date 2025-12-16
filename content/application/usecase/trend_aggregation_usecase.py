import os
from collections import defaultdict
from datetime import date, timedelta
from typing import Iterable, Optional

from sqlalchemy import text

from content.application.port.content_repository_port import ContentRepositoryPort
from content.domain.category_trend import CategoryTrend
from content.domain.keyword_trend import KeywordTrend
from config.database.session import SessionLocal


class TrendAggregationUseCase:
    def __init__(self, repository: ContentRepositoryPort, session_factory=SessionLocal):
        self.repository = repository
        self.session_factory = session_factory

    def aggregate(
        self,
        as_of: Optional[date] = None,
        window_days: int = 7,
        platform: str | None = None,
        surge_growth_threshold: float | None = None,
    ) -> dict:
        """
        - as_of: 기준 일자 (default: 오늘)
        """
        as_of = as_of or date.today()
        from_date = as_of - timedelta(days=window_days - 1)
        prev_to = as_of - timedelta(days=window_days)
        prev_from = prev_to - timedelta(days=window_days - 1)

        surge_threshold = surge_growth_threshold
        if surge_threshold is None:
            try:
                surge_threshold = float(os.getenv("SURGE_GROWTH_THRESHOLD", "1.0"))
            except ValueError:
                surge_threshold = 1.0

        with self.session_factory() as db:
            keyword_rows = self._aggregate_keywords(db, from_date, as_of, platform)
            keyword_prev_rows = self._aggregate_keywords(db, prev_from, prev_to, platform)
            category_rows = self._aggregate_categories(db, from_date, as_of, platform)
            category_prev_rows = self._aggregate_categories(db, prev_from, prev_to, platform)

        keyword_rows = self._attach_growth(keyword_rows, keyword_prev_rows, key_fields=("keyword", "platform"))
        category_rows = self._attach_growth(category_rows, category_prev_rows, key_fields=("category", "platform"))

        keyword_ranked = self._apply_rank(keyword_rows)
        category_ranked = self._apply_rank(category_rows)

        for row in keyword_ranked:
            trend = KeywordTrend(
                keyword=row["keyword"],
                date=as_of,
                platform=row["platform"],
                search_volume=row["search_volume"],
                search_volume_prev=row.get("search_volume_prev"),
                video_count=row["video_count"],
                video_count_prev=row.get("video_count_prev"),
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                growth_rate=row.get("growth_rate"),
                rank=row["rank"],
            )
            self.repository.upsert_keyword_trend(trend)

        for row in category_ranked:
            trend = CategoryTrend(
                category=row["category"],
                date=as_of,
                platform=row["platform"],
                video_count=row["video_count"],
                video_count_prev=row.get("video_count_prev"),
                avg_sentiment=row["avg_sentiment"],
                avg_trend=row["avg_trend"],
                avg_total_score=row["avg_total_score"],
                search_volume=row["search_volume"],
                search_volume_prev=row.get("search_volume_prev"),
                growth_rate=row.get("growth_rate"),
                rank=row["rank"],
            )
            self.repository.upsert_category_trend(trend)

        surging_keywords = [
            row
            for row in keyword_ranked
            if row.get("growth_rate") is not None and row.get("growth_rate", 0) >= surge_threshold
        ]
        surging_categories = [
            row
            for row in category_ranked
            if row.get("growth_rate") is not None and row.get("growth_rate", 0) >= surge_threshold
        ]

        return {
            "as_of": str(as_of),
            "keyword_trend_count": len(keyword_ranked),
            "category_trend_count": len(category_ranked),
            "surging_keywords": surging_keywords,
            "surging_categories": surging_categories,
        }

    def _aggregate_keywords(self, db, from_date: date, as_of: date, platform: str | None) -> list[dict]:
        rows = db.execute(
            text(
                """
                SELECT
                    km.keyword,
                    v.platform,
                    COUNT(DISTINCT km.video_id) AS video_count,
                    SUM(COALESCE(v.view_count, 0)) AS search_volume,
                    AVG(COALESCE(vs.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(vs.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(sc.total_score, 0)) AS avg_total_score
                FROM keyword_mapping km
                JOIN video v ON v.video_id = km.video_id
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE v.crawled_at::date BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                GROUP BY km.keyword, v.platform
                """
            ),
            {"from_date": from_date, "to_date": as_of, "platform": platform},
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "keyword": r["keyword"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _aggregate_categories(self, db, from_date: date, as_of: date, platform: str | None) -> list[dict]:
        rows = db.execute(
            text(
                """
                SELECT
                    v.platform,
                    COUNT(DISTINCT v.video_id) AS video_count,
                    SUM(COALESCE(v.view_count, 0)) AS search_volume,
                    AVG(COALESCE(vs.sentiment_score, 0)) AS avg_sentiment,
                    AVG(COALESCE(vs.trend_score, 0)) AS avg_trend,
                    AVG(COALESCE(sc.total_score, 0)) AS avg_total_score
                WHERE v.crawled_at::date BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                """
            ),
            {"from_date": from_date, "to_date": as_of, "platform": platform},
        ).mappings()

        result: list[dict] = []
        for r in rows:
            result.append(
                {
                    "category": r["category"],
                    "platform": r["platform"],
                    "video_count": int(r["video_count"] or 0),
                    "search_volume": int(r["search_volume"] or 0),
                    "avg_sentiment": float(r["avg_sentiment"] or 0),
                    "avg_trend": float(r["avg_trend"] or 0),
                    "avg_total_score": float(r["avg_total_score"] or 0),
                }
            )
        return result

    def _apply_rank(self, rows: Iterable[dict]) -> list[dict]:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            grouped[r["platform"]].append(r)

        ranked: list[dict] = []
        for platform, items in grouped.items():
            items_sorted = sorted(items, key=lambda x: x["search_volume"], reverse=True)
            for idx, item in enumerate(items_sorted, start=1):
                item_with_rank = dict(item)
                item_with_rank["rank"] = idx
                ranked.append(item_with_rank)
        return ranked

    def _attach_growth(
        self, current_rows: list[dict], prev_rows: list[dict], key_fields: tuple[str, str]
    ) -> list[dict]:
        """
        """
        prev_map: dict[tuple[str, str], dict] = {}
        for row in prev_rows:
            key = tuple(row[k] for k in key_fields)
            prev_map[key] = row

        enriched: list[dict] = []
        for row in current_rows:
            key = tuple(row[k] for k in key_fields)
            prev = prev_map.get(key, {})
            prev_volume = int(prev.get("search_volume") or 0)
            prev_count = int(prev.get("video_count") or 0)
            base_volume = prev_volume if prev_volume > 0 else 1
            growth_rate = (row["search_volume"] - prev_volume) / base_volume if (row["search_volume"] or prev_volume) else 0

            row_enriched = dict(row)
            row_enriched["search_volume_prev"] = prev_volume
            row_enriched["video_count_prev"] = prev_count
            row_enriched["growth_rate"] = growth_rate
            enriched.append(row_enriched)
        return enriched
