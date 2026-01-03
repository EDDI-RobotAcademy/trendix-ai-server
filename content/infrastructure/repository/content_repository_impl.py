from typing import Any, Iterable
from datetime import datetime, timedelta

from sqlalchemy import text

from config.database.session import SessionLocal
from content.application.port.content_repository_port import ContentRepositoryPort
from content.domain.channel import Channel
from content.domain.comment_sentiment import CommentSentiment
from content.domain.crawl_log import CrawlLog
from content.domain.creator_account import CreatorAccount
from content.domain.keyword_mapping import KeywordMapping
from content.domain.keyword_trend import KeywordTrend
from content.domain.category_trend import CategoryTrend
from content.domain.video import Video
from content.domain.video_comment import VideoComment
from content.domain.video_score import VideoScore
from content.domain.video_sentiment import VideoSentiment
from content.domain.video_metrics_snapshot import VideoMetricsSnapshot
from content.infrastructure.orm.models import (
    ChannelORM,
    CreatorAccountORM,
    VideoORM,
    VideoCommentORM,
    VideoSentimentORM,
    CommentSentimentORM,
    KeywordTrendORM,
    CategoryTrendORM,
    KeywordMappingORM,
    VideoScoreORM,
    CrawlLogORM,
    VideoMetricsSnapshotORM,
)


class ContentRepositoryImpl(ContentRepositoryPort):
    def __init__(self):
        self.db = SessionLocal()

    def upsert_channel(self, channel: Channel) -> Channel:
        orm = self.db.get(ChannelORM, channel.channel_id)
        if orm is None:
            orm = ChannelORM(channel_id=channel.channel_id)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 메타데이터 전체를 저장합니다.
            orm.platform = channel.platform or "youtube"
            orm.title = channel.title
            orm.description = channel.description
            orm.country = channel.country
            orm.created_at = channel.created_at
        # 한국어 주석: 기존 레코드는 변동성 필드(구독/조회/영상 수, 수집시각)만 최신화합니다.
        orm.subscriber_count = channel.subscriber_count
        orm.view_count = channel.view_count
        orm.video_count = channel.video_count
        orm.crawled_at = channel.crawled_at

        self.db.commit()
        return channel

    def upsert_account(self, account: CreatorAccount) -> CreatorAccount:
        orm = (
            self.db.query(CreatorAccountORM)
            .filter(
                CreatorAccountORM.account_id == account.account_id,
                CreatorAccountORM.platform == account.platform,
            )
            .one_or_none()
        )
        if orm is None:
            orm = CreatorAccountORM(account_id=account.account_id, platform=account.platform)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 프로필 정보를 저장합니다.
            orm.display_name = account.display_name
            orm.username = account.username
            orm.profile_url = account.profile_url
            orm.description = account.description
            orm.country = account.country
        # 한국어 주석: 변동성 필드(팔로워/게시물 수, 최신 업데이트 시간)만 갱신합니다.
        orm.follower_count = account.follower_count
        orm.post_count = account.post_count
        orm.last_updated_at = account.last_updated_at
        orm.crawled_at = account.crawled_at
        self.db.commit()
        return account

    def upsert_video(self, video: Video) -> Video:
        orm = self.db.get(VideoORM, video.video_id)
        if orm is None:
            orm = VideoORM(video_id=video.video_id)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 메타데이터를 저장하여 이후 변동성 업데이트와 분리합니다.
            orm.platform = video.platform or "youtube"
            orm.channel_id = video.channel_id
            orm.title = video.title
            orm.description = video.description
            orm.tags = video.tags
            orm.category_id = video.category_id
            orm.published_at = video.published_at
            orm.duration = video.duration
            orm.thumbnail_url = video.thumbnail_url
        # 한국어 주석: 기존 레코드는 변동성 필드(조회/좋아요/댓글 수, 최신 수집시각)만 갱신합니다.
        orm.view_count = video.view_count
        orm.like_count = video.like_count
        orm.comment_count = video.comment_count
        orm.crawled_at = video.crawled_at

        self.db.commit()
        return video

    def upsert_comments(self, comments: Iterable[VideoComment]) -> None:
        for comment in comments:
            orm = self.db.get(VideoCommentORM, comment.comment_id)
            if orm is None:
                orm = VideoCommentORM(comment_id=comment.comment_id)
                self.db.add(orm)
                # 한국어 주석: 최초 적재 시 댓글 본문/작성자 등 정적 정보를 저장합니다.
                orm.platform = comment.platform or "youtube"
                orm.video_id = comment.video_id
                orm.author = comment.author
                orm.content = comment.content
                orm.published_at = comment.published_at
            # 한국어 주석: 기존 댓글은 변동성 필드(좋아요 수)만 업데이트합니다.
            orm.like_count = comment.like_count
        self.db.commit()

    def upsert_video_sentiment(self, sentiment: VideoSentiment) -> VideoSentiment:
        orm = self.db.get(VideoSentimentORM, sentiment.video_id)
        if orm is None:
            orm = VideoSentimentORM(video_id=sentiment.video_id)
            self.db.add(orm)
        orm.platform = sentiment.platform or "youtube"
        orm.category = sentiment.category
        orm.trend_score = sentiment.trend_score
        orm.sentiment_label = sentiment.sentiment_label
        orm.sentiment_score = sentiment.sentiment_score
        orm.keywords = sentiment.keywords
        orm.summary = sentiment.summary
        orm.analyzed_at = sentiment.analyzed_at
        self.db.commit()
        return sentiment

    def upsert_comment_sentiments(self, sentiments: Iterable[CommentSentiment]) -> None:
        for sentiment in sentiments:
            orm = self.db.get(CommentSentimentORM, sentiment.comment_id)
            if orm is None:
                orm = CommentSentimentORM(comment_id=sentiment.comment_id)
                self.db.add(orm)
            orm.platform = sentiment.platform or "youtube"
            orm.sentiment_label = sentiment.sentiment_label
            orm.sentiment_score = sentiment.sentiment_score
            orm.analyzed_at = sentiment.analyzed_at
        self.db.commit()

    def upsert_keyword_trend(self, trend: KeywordTrend) -> KeywordTrend:
        orm = (
            self.db.query(KeywordTrendORM)
            .filter(
                KeywordTrendORM.keyword == trend.keyword,
                KeywordTrendORM.date == trend.date,
                KeywordTrendORM.platform == trend.platform,
            )
            .one_or_none()
        )
        if orm is None:
            orm = KeywordTrendORM(
                keyword=trend.keyword, date=trend.date, platform=trend.platform
            )
            self.db.add(orm)
        orm.search_volume = trend.search_volume
        orm.search_volume_prev = trend.search_volume_prev
        orm.video_count = trend.video_count
        orm.video_count_prev = trend.video_count_prev
        orm.avg_sentiment = trend.avg_sentiment
        orm.avg_trend = trend.avg_trend
        orm.avg_total_score = trend.avg_total_score
        orm.growth_rate = trend.growth_rate
        orm.rank = trend.rank
        self.db.commit()
        return trend

    def upsert_category_trend(self, trend: CategoryTrend) -> CategoryTrend:
        orm = (
            self.db.query(CategoryTrendORM)
            .filter(
                CategoryTrendORM.category == trend.category,
                CategoryTrendORM.date == trend.date,
                CategoryTrendORM.platform == trend.platform,
            )
            .one_or_none()
        )
        if orm is None:
            orm = CategoryTrendORM(
                category=trend.category, date=trend.date, platform=trend.platform
            )
            self.db.add(orm)
        orm.video_count = trend.video_count
        orm.video_count_prev = trend.video_count_prev
        orm.avg_sentiment = trend.avg_sentiment
        orm.avg_trend = trend.avg_trend
        orm.avg_total_score = trend.avg_total_score
        orm.search_volume = trend.search_volume
        orm.search_volume_prev = trend.search_volume_prev
        orm.growth_rate = trend.growth_rate
        orm.rank = trend.rank
        self.db.commit()
        return trend

    def upsert_keyword_mapping(self, mapping: KeywordMapping) -> KeywordMapping:
        # 동일 (video_id, keyword, platform) 조합 중복 삽입을 막기 위해 조회 후 갱신/신규 생성
        platform = mapping.platform or "youtube"
        orm = None
        if mapping.video_id and mapping.keyword:
            orm = (
                self.db.query(KeywordMappingORM)
                .filter(
                    KeywordMappingORM.video_id == mapping.video_id,
                    KeywordMappingORM.keyword == mapping.keyword,
                    KeywordMappingORM.platform == platform,
                )
                .one_or_none()
            )
        if orm is None:
            orm = KeywordMappingORM()
            self.db.add(orm)
        orm.platform = platform
        orm.video_id = mapping.video_id
        orm.channel_id = mapping.channel_id
        orm.keyword = mapping.keyword
        orm.weight = mapping.weight
        self.db.commit()
        mapping.mapping_id = getattr(orm, "mapping_id", None)
        return mapping

    def upsert_video_score(self, score: VideoScore) -> VideoScore:
        orm = self.db.get(VideoScoreORM, score.video_id)
        if orm is None:
            orm = VideoScoreORM(video_id=score.video_id)
            self.db.add(orm)
        orm.platform = score.platform or "youtube"
        orm.engagement_score = score.engagement_score
        orm.sentiment_score = score.sentiment_score
        orm.trend_score = score.trend_score
        orm.total_score = score.total_score
        orm.updated_at = score.updated_at
        self.db.commit()
        return score

    def log_crawl(self, log: CrawlLog) -> CrawlLog:
        orm = CrawlLogORM(
            target_type=log.target_type,
            target_id=log.target_id,
            status=log.status,
            message=log.message,
            crawled_at=log.crawled_at,
        )
        self.db.add(orm)
        self.db.commit()
        log.id = orm.id
        return log

    def upsert_video_metrics_snapshot(self, snapshot: VideoMetricsSnapshot) -> None:
        """
        일별 영상 지표 스냅샷을 upsert합니다. 동일 (video_id, snapshot_date, platform) 키에 대해서는 값을 갱신합니다.
        """
        # NOTE: SQLAlchemy ORM보다 ON CONFLICT가 명확한 raw SQL을 사용합니다.
        self.db.execute(
            text(
                """
                INSERT INTO video_metrics_snapshot (video_id, platform, snapshot_date, view_count, like_count, comment_count)
                VALUES (:video_id, :platform, :snapshot_date, :view_count, :like_count, :comment_count)
                ON CONFLICT (video_id, snapshot_date, platform)
                DO UPDATE SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count
                """
            ),
            {
                "video_id": snapshot.video_id,
                "platform": snapshot.platform or "youtube",
                "snapshot_date": snapshot.snapshot_date,
                "view_count": snapshot.view_count,
                "like_count": snapshot.like_count,
                "comment_count": snapshot.comment_count,
            },
        )
        self.db.commit()

    def fetch_videos_by_category(self, category: str, limit: int = 20) -> list[dict]:
        """
        카테고리 기준 상위 콘텐츠를 점수/조회수 기반으로 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.channel_id,
                    v.platform,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.thumbnail_url,
                    v.category_id,
                    vs.category,
                    vs.sentiment_label,
                    vs.sentiment_score,
                    vs.trend_score,
                    sc.engagement_score,
                    sc.sentiment_score AS score_sentiment,
                    sc.trend_score AS score_trend,
                    sc.total_score
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE vs.category = :category
                ORDER BY COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, v.view_count) DESC NULLS LAST,
                         v.crawled_at DESC
                LIMIT :limit
                """
            ),
            {"category": category, "limit": limit},
        ).mappings()
        return [dict(row) for row in rows]

    def fetch_videos_by_category_id(
        self, category_id: int, limit: int = 10, platform: str | None = None, days: int | None = None
    ) -> list[dict]:
        """
        YouTube category_id 기준 상위 콘텐츠를 조회한다.
        - category_id: YouTube Data API의 숫자 categoryId (예: 10=Music, 20=Gaming)
        - days: 최근 N일 내 게시된 영상만 대상 (None이면 전체)
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        since_date = None
        until_date = None
        if days is not None:
            since_date = (datetime.utcnow() - timedelta(days=days)).date()
            until_date = datetime.utcnow().date()
        
        rows = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.description,
                    v.tags,
                    v.category_id,
                    v.duration,
                    v.channel_id,
                    v.platform,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.thumbnail_url,
                    v.crawled_at,
                    v.is_shorts,
                    vs.category,
                    vs.sentiment_label,
                    vs.sentiment_score,
                    vs.trend_score,
                    sc.engagement_score,
                    sc.sentiment_score AS score_sentiment,
                    sc.trend_score AS score_trend,
                    sc.total_score
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE v.category_id = :category_id
                  AND (:platform IS NULL OR v.platform = :platform)
                  AND (:since_date IS NULL OR v.published_at::date >= :since_date)
                  AND (:until_date IS NULL OR v.published_at::date <= :until_date)
                ORDER BY COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, v.view_count) DESC NULLS LAST,
                         v.crawled_at DESC
                LIMIT :limit
                """
            ),
            {
                "category_id": category_id,
                "platform": platform,
                "limit": limit,
                "since_date": since_date,
                "until_date": until_date,
            },
        ).mappings()
        return [dict(row) for row in rows]

    def fetch_video_view_history(
        self,
        video_id: str,
        platform: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        video_metrics_snapshot 기준으로 단일 영상의 히스토리를 조회한다.
        - snapshot_date 내림차순 정렬
        """
        try:
            self.db.rollback()
        except Exception:
            pass

        sql = """
            SELECT
                video_id,
                platform,
                snapshot_date,
                view_count,
                like_count,
                comment_count
            FROM video_metrics_snapshot
            WHERE video_id = :video_id
              AND (:platform IS NULL OR platform = :platform)
            ORDER BY snapshot_date DESC
        """
        if limit is not None:
            sql += " LIMIT :limit"

        params: dict[str, Any] = {
            "video_id": video_id,
            "platform": platform,
        }
        if limit is not None:
            params["limit"] = limit

        rows = self.db.execute(text(sql), params).mappings()
        return [dict(r) for r in rows]

    def fetch_videos_by_keyword(self, keyword: str, limit: int = 20) -> list[dict]:
        """
        키워드 기준 상위 콘텐츠를 점수/조회수 기반으로 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.channel_id,
                    v.platform,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.thumbnail_url,
                    vs.category,
                    vs.sentiment_label,
                    vs.sentiment_score,
                    vs.trend_score,
                    sc.engagement_score,
                    sc.sentiment_score AS score_sentiment,
                    sc.trend_score AS score_trend,
                    sc.total_score
                FROM keyword_mapping km
                JOIN video v ON v.video_id = km.video_id
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE km.keyword = :keyword
                ORDER BY COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, v.view_count) DESC NULLS LAST,
                         v.crawled_at DESC
                LIMIT :limit
                """
            ),
            {"keyword": keyword, "limit": limit},
        ).mappings()
        return [dict(row) for row in rows]

    def fetch_top_keywords_by_category(self, category: str, limit: int = 10) -> list[dict]:
        """
        특정 카테고리 내 콘텐츠에서 많이 등장한 주요 키워드를 빈도순으로 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                SELECT
                    km.keyword,
                    COUNT(DISTINCT km.video_id) AS video_count
                FROM keyword_mapping km
                JOIN video_sentiment vs ON vs.video_id = km.video_id
                WHERE vs.category = :category
                GROUP BY km.keyword
                ORDER BY COUNT(DISTINCT km.video_id) DESC, km.keyword
                LIMIT :limit
                """
            ),
            {"category": category, "limit": limit},
        ).mappings()
        return [dict(row) for row in rows]

    def fetch_top_keywords_by_keyword(self, keyword: str, limit: int = 10) -> list[dict]:
        """
        특정 키워드와 함께 등장한 연관 키워드를 빈도순으로 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                SELECT
                    km2.keyword,
                    COUNT(DISTINCT km2.video_id) AS video_count
                FROM keyword_mapping km_target
                JOIN keyword_mapping km2 ON km_target.video_id = km2.video_id
                WHERE km_target.keyword = :keyword
                  AND km2.keyword <> :keyword
                GROUP BY km2.keyword
                ORDER BY COUNT(DISTINCT km2.video_id) DESC, km2.keyword
                LIMIT :limit
                """
            ),
            {"keyword": keyword, "limit": limit},
        ).mappings()
        return [dict(row) for row in rows]

    def fetch_video_with_scores(self, video_id: str) -> dict | None:
        """
        콘텐츠 단건 상세(점수/키워드 포함)를 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        video = self.db.execute(
            text(
                """
                SELECT v.video_id, v.title, v.channel_id, v.platform, v.view_count, v.like_count, v.comment_count,
                       v.published_at, v.thumbnail_url,
                       vs.category, vs.sentiment_label, vs.sentiment_score, vs.trend_score, vs.keywords, vs.summary,
                       sc.engagement_score, sc.sentiment_score AS score_sentiment, sc.trend_score AS score_trend, sc.total_score,
                       vs.analyzed_at
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                WHERE v.video_id = :video_id
                """
            ),
            {"video_id": video_id},
        ).mappings().first()

        if not video:
            return None

        keywords = self.db.execute(
            text(
                """
                SELECT keyword, weight, platform, video_id, channel_id
                FROM keyword_mapping
                WHERE video_id = :video_id
                ORDER BY weight DESC NULLS LAST, keyword
                """
            ),
            {"video_id": video_id},
        ).mappings().all()

        return {"video": dict(video), "keywords": [dict(k) for k in keywords]}

    def fetch_hot_category_trends(self, platform: str | None = None, limit: int = 20) -> list[dict]:
        """
        최신 집계 일자의 카테고리별 랭킹을 반환한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                 SELECT ct.category,
                       ct.platform,
                       ct.date,
                       ct.video_count,
                       ct.video_count_prev,
                       ct.avg_sentiment,
                       ct.avg_trend,
                       ct.avg_total_score,
                       ct.search_volume,
                       ct.search_volume_prev,
                       ct.growth_rate,
                       ct.rank
                FROM category_trend ct
                JOIN (
                    SELECT category, platform, MAX(date) AS max_date
                    FROM category_trend
                    WHERE (:platform IS NULL OR platform = :platform)
                    GROUP BY category, platform
                ) latest
                  ON ct.category = latest.category
                 AND ct.platform = latest.platform
                 AND ct.date = latest.max_date
                WHERE (:platform IS NULL OR ct.platform = :platform)
                ORDER BY ct.rank ASC NULLS LAST, ct.search_volume DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {"platform": platform, "limit": limit},
        ).mappings()
        return [dict(r) for r in rows]

    def fetch_popular_videos(self, limit: int = 5, platform: str | None = None) -> list[dict]:
        """
        절대 인기 상위 리스트 (조회수 중심, 좋아요/스코어 보조).
        채널 규모 편향 보정: 채널 평균 조회수를 나눈 정규화 점수를 함께 반환.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                WITH base AS (
                    SELECT
                        v.*,
                        vs.category,
                        vs.sentiment_label,
                        vs.sentiment_score,
                        vs.trend_score,
                        sc.engagement_score,
                        sc.sentiment_score AS score_sentiment,
                        sc.trend_score AS score_trend,
                        sc.total_score,
                        AVG(v.view_count) OVER (PARTITION BY v.channel_id) AS channel_avg_view
                    FROM video v
                    LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                    LEFT JOIN video_score sc ON sc.video_id = v.video_id
                    WHERE (:platform IS NULL OR v.platform = :platform)
                )
                SELECT
                    video_id,
                    title,
                    description,
                    tags,
                    category_id,
                    duration,
                    channel_id,
                    platform,
                    view_count,
                    like_count,
                    comment_count,
                    published_at,
                    thumbnail_url,
                    category,
                    sentiment_label,
                    sentiment_score,
                    trend_score,
                    engagement_score,
                    score_sentiment,
                    score_trend,
                    total_score,
                    crawled_at,
                    is_shorts,
                    channel_avg_view,
                    CASE
                        WHEN channel_avg_view > 0 THEN view_count / channel_avg_view
                        ELSE view_count
                    END AS normalized_view_score
                FROM base
                ORDER BY normalized_view_score DESC NULLS LAST,
                         COALESCE(total_score, view_count, score_sentiment, score_trend) DESC NULLS LAST,
                         view_count DESC NULLS LAST,
                         crawled_at DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {"platform": platform, "limit": limit},
        ).mappings()
        return [dict(r) for r in rows]

    def fetch_rising_videos(self, limit: int = 5, velocity_days: int = 1, platform: str | None = None) -> list[dict]:
        """
        최근 velocity(조회 증가량/일)를 기반한 급상승 리스트 + 채널 규모 보정 점수 포함.
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                WITH latest AS (
                    SELECT
                        v.video_id,
                        v.title,
                        v.description,
                        v.tags,
                        v.category_id,
                        v.duration,
                        v.channel_id,
                        v.platform,
                        v.view_count,
                        v.like_count,
                        v.comment_count,
                        v.published_at,
                        v.thumbnail_url,
                        v.crawled_at,
                        vs.category,
                        vs.sentiment_label,
                        vs.sentiment_score,
                        vs.trend_score,
                        sc.engagement_score,
                        sc.sentiment_score AS score_sentiment,
                        sc.trend_score AS score_trend,
                        sc.total_score,
                        v.is_shorts,
                        AVG(v.view_count) OVER (PARTITION BY v.channel_id) AS channel_avg_view
                    FROM video v
                    LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                    LEFT JOIN video_score sc ON sc.video_id = v.video_id
                    WHERE (:platform IS NULL OR v.platform = :platform)
                ),
                curr AS (
                    SELECT DISTINCT ON (vms.video_id)
                        vms.video_id,
                        vms.view_count,
                        vms.snapshot_date
                    FROM video_metrics_snapshot vms
                    WHERE (:platform IS NULL OR vms.platform = :platform)
                    ORDER BY vms.video_id, vms.snapshot_date DESC
                ),
                prev AS (
                    SELECT DISTINCT ON (vms.video_id)
                        vms.video_id,
                        vms.view_count,
                        vms.snapshot_date
                    FROM video_metrics_snapshot vms
                    WHERE (:platform IS NULL OR vms.platform = :platform)
                      AND vms.snapshot_date <= (CURRENT_DATE - (:velocity_days || ' days')::interval)
                    ORDER BY vms.video_id, vms.snapshot_date DESC
                )
                SELECT
                    l.*,
                    GREATEST(COALESCE(c.view_count, l.view_count, 0) - COALESCE(p.view_count, 0), 0) / NULLIF(:velocity_days,0) AS view_velocity,
                    CASE
                        WHEN channel_avg_view > 0 THEN l.view_count / channel_avg_view
                        ELSE l.view_count
                    END AS normalized_view_score
                FROM latest l
                LEFT JOIN curr c ON c.video_id = l.video_id
                LEFT JOIN prev p ON p.video_id = l.video_id
                ORDER BY view_velocity DESC NULLS LAST,
                         normalized_view_score DESC NULLS LAST,
                         COALESCE(l.total_score, l.view_count) DESC NULLS LAST,
                         l.crawled_at DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {"platform": platform, "limit": limit, "velocity_days": velocity_days},
        ).mappings()
        return [dict(r) for r in rows]

    def fetch_recommended_videos_by_category(
        self, category: str, limit: int = 20, days: int = 14, platform: str | None = None
    ) -> list[dict]:
        """
        카테고리 문자열(category) 기준으로 최근 수집 콘텐츠를 점수 기반으로 추천한다.
        """
        # 이전 예외로 인한 pending rollback 상태 방지
        try:
            self.db.rollback()
        except Exception:
            pass
        # days 파라미터는 "최근 N일간 게시된 영상"을 의미하도록, 수집 시점(crawled_at)이 아닌 게시 시점(published_at)으로 필터링한다.
            # days 파라미터는 "최근 N일간 게시된 영상"을 의미하도록, 수집 시점(crawled_at)이 아닌 게시 시점(published_at)으로 필터링한다.
        since_date = (datetime.utcnow() - timedelta(days=days)).date()
        until_date = datetime.utcnow().date()
        rows = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.description,
                    v.tags,
                    v.category_id,
                    v.duration,
                    v.channel_id,
                    v.platform,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.thumbnail_url,
                    v.crawled_at,
                    v.is_shorts,
                    vs.category,
                    vs.sentiment_label,
                    vs.sentiment_score,
                    vs.trend_score,
                    sc.engagement_score,
                    sc.sentiment_score AS score_sentiment,
                    sc.trend_score AS score_trend,
                    sc.total_score,
                    -- username(또는 display_name/title)을 단 한 번만 @로 prefix 하여 프런트에 바로 전달
                    CASE
                        WHEN COALESCE(ca.username, ca.display_name, ch.title, v.channel_id) LIKE '@%' THEN COALESCE(ca.username, ca.display_name, ch.title, v.channel_id)
                        ELSE '@' || COALESCE(ca.username, ca.display_name, ch.title, v.channel_id)
                    END AS channel_username
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN creator_account ca ON ca.account_id = v.channel_id AND ca.platform = v.platform
                LEFT JOIN channel ch ON ch.channel_id = v.channel_id
                WHERE vs.category = :category
                  AND v.published_at::date BETWEEN :since_date AND :until_date
                  AND (:platform IS NULL OR v.platform = :platform)
                ORDER BY COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, v.view_count) DESC NULLS LAST,
                         v.crawled_at DESC
                LIMIT :limit
                """
            ),
            {
                "category": category,
                "since_date": since_date,
                "until_date": until_date,
                "platform": platform,
                "limit": limit,
            },
        ).mappings()
        return [dict(r) for r in rows]

    def fetch_distinct_categories(self, limit: int = 100) -> list[str]:
        """
        등록된 카테고리 목록만 조회(관심사 등록용).
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        rows = self.db.execute(
            text(
                """
                SELECT category FROM (
                    SELECT DISTINCT category FROM video_sentiment WHERE category IS NOT NULL
                    UNION
                    SELECT DISTINCT category FROM category_trend WHERE category IS NOT NULL
                ) c
                ORDER BY category
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).scalars()
        return list(rows)

    def fetch_surge_videos(
        self,
        platform: str | None = None,
        limit: int = 30,
        days: int = 3,
        velocity_days: int = 1,
    ) -> list[dict]:
        """
        단기 조회수 증가량/증가율(일 단위 스냅샷 기반)을 활용해 급등 영상 랭킹을 계산한다.

        - days: 최근 N일 내 업로드/수집된 영상만 대상
        - velocity_days: 이전 스냅샷 기준 일수 (예: 1일 전과 비교)
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        to_date = datetime.utcnow().date()
        from_date = to_date - timedelta(days=days - 1)
        prev_anchor = to_date - timedelta(days=velocity_days)

        rows = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.description,
                    v.tags,
                    v.category_id,
                    v.duration,
                    v.channel_id,
                    v.platform,
                    vs.category,
                    COALESCE(curr.view_count, v.view_count, 0) AS view_count,
                    COALESCE(prev.view_count, 0) AS view_count_prev,
                    COALESCE(curr.like_count, v.like_count, 0) AS like_count,
                    COALESCE(prev.like_count, 0) AS like_count_prev,
                    COALESCE(curr.comment_count, v.comment_count, 0) AS comment_count,
                    COALESCE(prev.comment_count, 0) AS comment_count_prev,
                    (COALESCE(curr.view_count, v.view_count, 0) - COALESCE(prev.view_count, 0)) / :velocity_days AS view_velocity,
                    (COALESCE(curr.like_count, v.like_count, 0) - COALESCE(prev.like_count, 0)) / :velocity_days AS like_velocity,
                    (COALESCE(curr.comment_count, v.comment_count, 0) - COALESCE(prev.comment_count, 0)) / :velocity_days AS comment_velocity,
                    COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, 0) AS total_score,
                    v.published_at,
                    v.thumbnail_url,
                    v.crawled_at,
                    v.is_shorts
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :to_date
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) curr ON true
                LEFT JOIN LATERAL (
                    SELECT s.view_count, s.like_count, s.comment_count
                    FROM video_metrics_snapshot s
                    WHERE s.video_id = v.video_id
                      AND s.platform = v.platform
                      AND s.snapshot_date <= :prev_anchor
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) prev ON true
                WHERE COALESCE(v.published_at::date, v.crawled_at::date) BETWEEN :from_date AND :to_date
                  AND (:platform IS NULL OR v.platform = :platform)
                ORDER BY 
                    -- 최우선: velocity가 있는 영상들을 상위에 배치
                    CASE WHEN COALESCE(curr.view_count, v.view_count, 0) - COALESCE(prev.view_count, 0) > 0 THEN 1 ELSE 0 END DESC,
                    -- velocity 기반 정렬
                    view_velocity DESC NULLS LAST,
                    comment_velocity DESC NULLS LAST, 
                    like_velocity DESC NULLS LAST,
                    -- fallback: velocity 데이터가 없는 경우 절대값 기준
                    COALESCE(curr.view_count, v.view_count, 0) DESC,
                    total_score DESC NULLS LAST,
                    v.published_at DESC NULLS LAST
                LIMIT :limit
                """
            ),
            {
                "from_date": from_date,
                "to_date": to_date,
                "prev_anchor": prev_anchor,
                "platform": platform,
                "velocity_days": velocity_days,
                "limit": limit,
            },
        ).mappings()

        import math
        
        now = datetime.utcnow()
        result: list[dict] = []

        # 1차로 SQL 정렬 기준에 따라 surge_score를 계산해 result 리스트를 만든다.
        for r in rows:
            view_now = int(r["view_count"] or 0)
            view_prev = int(r["view_count_prev"] or 0)
            delta_views = view_now - view_prev
            base_views = view_prev if view_prev > 0 else 1
            growth_rate = delta_views / base_views if (view_now or view_prev) else 0.0

            published_at = r.get("published_at")
            
            # === 업로드 경과 시간(Freshness) 계산 ===
            if published_at is not None:
                # 현재 시각 - 업로드 시각 = 경과 시간
                time_elapsed = now - published_at
                age_seconds = max(time_elapsed.total_seconds(), 0.0)
                age_minutes = age_seconds / 60.0
                age_hours = age_minutes / 60.0
                age_days = age_hours / 24.0
                
                # Freshness Score: 최신 콘텐츠일수록 높은 점수
                # 지수 감쇠 함수 사용: freshness = exp(-λ * age_hours)
                # λ = 0.05 → 24시간 후 약 0.30, 48시간 후 약 0.09
                freshness_decay_rate = 0.05
                freshness_score = math.exp(-freshness_decay_rate * age_hours)
                
                # 추가 보너스: 24시간 이내 업로드는 추가 가중치
                if age_hours <= 24:
                    freshness_bonus = 1.5
                elif age_hours <= 48:
                    freshness_bonus = 1.2
                elif age_hours <= 72:
                    freshness_bonus = 1.1
                else:
                    freshness_bonus = 1.0
                
                freshness_score_with_bonus = freshness_score * freshness_bonus
            else:
                # 업로드 시각이 없는 경우 기본값
                age_seconds = None
                age_minutes = None
                age_hours = None
                age_days = None
                freshness_score = 0.5  # 중간 값
                freshness_bonus = 1.0
                freshness_score_with_bonus = 0.5

            item = dict(r)
            
            # 경과 시간 정보
            item["age_seconds"] = age_seconds
            item["age_minutes"] = age_minutes
            item["age_hours"] = age_hours
            item["age_days"] = age_days
            
            # Freshness 점수
            item["freshness_score"] = round(freshness_score, 4)
            item["freshness_bonus"] = freshness_bonus
            item["freshness_score_with_bonus"] = round(freshness_score_with_bonus, 4)
            
            # 증가 지표
            item["delta_views_window"] = float(delta_views)
            item["growth_rate_window"] = float(growth_rate)
            
            item["age_minutes"] = age_minutes
            item["age_hours"] = age_hours
            # === 개선된 급등 점수(Surge Score) 계산 ===
            # 요소 1: 현재 인기도 (로그 스케일)
            base_popularity = math.log(max(view_now, 1) + 10)
            
            # 요소 2: 시간당 증가량 (velocity) 정규화
            velocity_factor = float(r["view_velocity"] or 0) / 1000.0
            
            # 요소 3: 성장률 (growth_rate) - 백분율로 변환
            growth_factor = growth_rate * 100
            
            # 요소 4: Freshness - 최신 콘텐츠에 가중치
            freshness_factor = freshness_score_with_bonus * 50  # 0~75 범위로 스케일링
            
            # 최종 Surge Score = 성장률 + velocity + 인기도 + Freshness
            surge_score = (
                growth_factor +           # 성장률 기여도
                velocity_factor +         # 절대 증가량 기여도
                (base_popularity * 0.1) + # 현재 인기도 기여도
                freshness_factor          # 신선도 기여도
            )
            
            item["surge_score"] = round(surge_score, 2)
            
            # 디버깅/분석용 세부 점수
            item["surge_components"] = {
                "growth_factor": round(growth_factor, 2),
                "velocity_factor": round(velocity_factor, 2),
                "popularity_factor": round(base_popularity * 0.1, 2),
                "freshness_factor": round(freshness_factor, 2),
            }

            result.append(item)

        # surge_score 기준으로 전체 내림차순 정렬 후 rank를 부여한다.
        result_sorted = sorted(
            result,
            key=lambda x: x.get("surge_score") or 0.0,
            reverse=True,
        )

        for idx, item in enumerate(result_sorted, 1):
            item["trending_rank"] = idx

            # video_score.trend_score에 surge_score를 upsert 한다.
            try:
                self.db.execute(
                    text(
                        """
                        INSERT INTO video_score (video_id, platform, trend_score, updated_at)
                        VALUES (:video_id, :platform, :trend_score, :updated_at)
                        ON CONFLICT (video_id) DO UPDATE SET
                            trend_score = EXCLUDED.trend_score,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    {
                        "video_id": item["video_id"],
                        "platform": item.get("platform") or "youtube",
                        "trend_score": item["surge_score"],
                        "updated_at": now,
                    },
                )
            except Exception:
                # 점수 저장 실패는 조회 자체를 막지 않기 위해 무시한다.
                pass

        try:
            self.db.commit()
        except Exception:
            # commit 에러도 조회 응답은 유지하되, 추후 로그/모니터링으로 대응
            self.db.rollback()

        return result_sorted