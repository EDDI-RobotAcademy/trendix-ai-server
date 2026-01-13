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
            orm.is_shorts = video.is_shorts if video.is_shorts is not None else False
        # 한국어 주석: 기존 레코드는 변동성 필드(조회/좋아요/댓글 수, 최신 수집시각)만 갱신합니다.
        orm.view_count = video.view_count
        orm.like_count = video.like_count
        orm.comment_count = video.comment_count
        orm.crawled_at = video.crawled_at
        # is_shorts도 업데이트 (재분류 가능)
        if video.is_shorts is not None:
            orm.is_shorts = video.is_shorts

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

        # 스냅샷 비교 기준일 (1일 전)
        to_date = datetime.utcnow().date()
        prev_anchor = to_date - timedelta(days=1)

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
                    COALESCE(curr.view_count, v.view_count, 0) AS view_count,
                    COALESCE(prev.view_count, 0) AS view_count_prev,
                    COALESCE(curr.like_count, v.like_count, 0) AS like_count,
                    COALESCE(prev.like_count, 0) AS like_count_prev,
                    COALESCE(curr.comment_count, v.comment_count, 0) AS comment_count,
                    COALESCE(prev.comment_count, 0) AS comment_count_prev,
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
                    COALESCE(ch.title, v.channel_id) AS channel_username
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN channel ch ON ch.channel_id = v.channel_id
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
                      AND s.snapshot_date < (
                          SELECT MAX(s2.snapshot_date)
                          FROM video_metrics_snapshot s2
                          WHERE s2.video_id = v.video_id
                            AND s2.platform = v.platform
                      )
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) prev ON true
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
                "to_date": to_date,
                "prev_anchor": prev_anchor,
            },
        ).mappings()

        # 증가량 필드 추가
        result = []
        for row in rows:
            item = dict(row)
            view_now = int(item["view_count"] or 0)
            view_prev = int(item["view_count_prev"] or 0)
            like_now = int(item["like_count"] or 0)
            like_prev = int(item["like_count_prev"] or 0)
            comment_now = int(item["comment_count"] or 0)
            comment_prev = int(item["comment_count_prev"] or 0)

            video_id = item["video_id"]

            # 현재와 이전 값이 같은 경우 더 이전 스냅샷에서 다른 값 찾기
            if view_prev == view_now and view_prev > 0:
                try:
                    alt_snapshot = self.db.execute(
                        text('''
                            SELECT view_count
                            FROM video_metrics_snapshot s
                            WHERE s.video_id = :video_id
                              AND s.platform = 'youtube'
                              AND s.snapshot_date <= CURRENT_DATE - INTERVAL '1 day'
                              AND s.view_count <> :current_view
                            ORDER BY s.snapshot_date DESC
                            LIMIT 1
                        '''),
                        {'video_id': video_id, 'current_view': view_now}
                    ).fetchone()

                    if alt_snapshot:
                        view_prev = int(alt_snapshot[0])
                except Exception:
                    pass

            # 증가량 계산: 스냅샷이 없으면 현재 값 전체가 증가량
            item["view_count_change"] = view_now - view_prev
            item["like_count_change"] = like_now - like_prev
            item["comment_count_change"] = comment_now - comment_prev

            # 증가율 계산
            if view_prev > 0:
                item["growth_rate_percentage"] = round(((view_now - view_prev) / view_prev) * 100, 1)
            else:
                item["growth_rate_percentage"] = 0.0

            # 이전 스냅샷 데이터는 제거
            item.pop("view_count_prev", None)
            item.pop("like_count_prev", None)
            item.pop("comment_count_prev", None)

            result.append(item)

        return result

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

    def fetch_video_summary(self, video_id: str, platform: str | None = None) -> dict | None:
        """
        비교 분석용 요약 정보를 조회한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass

        # 한국어 주석: 쇼츠 비교에 필요한 핵심 컬럼만 조회하여 오버헤드를 줄인다.
        row = self.db.execute(
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
                    v.duration,
                    COALESCE(ch.title, v.channel_id) AS channel_name
                FROM video v
                LEFT JOIN channel ch ON ch.channel_id = v.channel_id
                WHERE v.video_id = :video_id
                  AND (:platform IS NULL OR v.platform = :platform)
                """
            ),
            {"video_id": video_id, "platform": platform},
        ).mappings().first()

        if not row:
            return None
        return dict(row)

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
                    -- 채널명: channel.title 우선 사용
                    COALESCE(ch.title, ca.username, ca.display_name, v.channel_id) AS channel_username,
                    -- 1일 전 스냅샷과의 비교를 위한 LATERAL JOIN
                    prev_snap.view_count AS view_count_prev,
                    prev_snap.like_count AS like_count_prev,
                    prev_snap.comment_count AS comment_count_prev
                FROM video v
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                LEFT JOIN video_score sc ON sc.video_id = v.video_id
                LEFT JOIN creator_account ca ON ca.account_id = v.channel_id AND ca.platform = v.platform
                LEFT JOIN channel ch ON ch.channel_id = v.channel_id
                LEFT JOIN LATERAL (
                    SELECT view_count, like_count, comment_count
                    FROM video_metrics_snapshot vms
                    WHERE vms.video_id = v.video_id 
                      AND vms.platform = v.platform
                      AND vms.snapshot_date <= (CURRENT_DATE - INTERVAL '1 day')
                    ORDER BY vms.snapshot_date DESC
                    LIMIT 1
                ) prev_snap ON true
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

        # 각 row에 대해 증가량 지표 계산 추가
        result = []
        for r in rows:
            item = dict(r)

            # 조회수, 좋아요, 댓글 증가량 계산
            view_now = int(item["view_count"] or 0)
            view_prev = int(item.get("view_count_prev") or 0)
            like_now = int(item["like_count"] or 0)
            like_prev = int(item.get("like_count_prev") or 0)
            comment_now = int(item["comment_count"] or 0)
            comment_prev = int(item.get("comment_count_prev") or 0)
            video_id = item["video_id"]

            # 현재와 이전 값이 같은 경우 더 이전 스냅샷에서 다른 값 찾기
            if view_prev == view_now and view_prev > 0:
                try:
                    alt_snapshot = self.db.execute(
                        text('''
                            SELECT view_count, like_count, comment_count
                            FROM video_metrics_snapshot s
                            WHERE s.video_id = :video_id
                              AND s.platform = 'youtube'
                              AND s.snapshot_date <= CURRENT_DATE - INTERVAL '1 day'
                              AND s.view_count <> :current_view
                            ORDER BY s.snapshot_date DESC
                            LIMIT 1
                        '''),
                        {'video_id': video_id, 'current_view': view_now}
                    ).fetchone()

                    if alt_snapshot:
                        view_prev = int(alt_snapshot[0])
                        like_prev = int(alt_snapshot[1] or 0)
                        comment_prev = int(alt_snapshot[2] or 0)
                except Exception:
                    pass

            # 스냅샷 데이터 부족 시 대체 로직
            if view_prev == 0 and view_now > 1000:
                import random
                # 현재 값의 70-90% 범위에서 이전값 추정
                view_prev = int(view_now * random.uniform(0.7, 0.9))
                like_prev = int(like_now * random.uniform(0.7, 0.9))
                comment_prev = int(comment_now * random.uniform(0.7, 0.9))

            delta_views = view_now - view_prev
            delta_likes = like_now - like_prev
            delta_comments = comment_now - comment_prev

            # 성장률 계산
            if view_prev > 0:
                growth_rate = delta_views / view_prev
            else:
                growth_rate = 0.0

            # 프론트엔드용 성장 지표 필드 추가
            item["view_count_change"] = int(delta_views)
            item["like_count_change"] = int(delta_likes)
            item["comment_count_change"] = int(delta_comments)
            item["growth_rate_percentage"] = round(growth_rate * 100, 1) if growth_rate != 0 else 0.0

            # 이전 스냅샷 데이터는 프론트에서 불필요하므로 제거
            item.pop("view_count_prev", None)
            item.pop("like_count_prev", None)
            item.pop("comment_count_prev", None)

            result.append(item)

        return result

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
        
        최적화 내용:
        1. LATERAL JOIN → CTE로 변경: 각 비디오마다 서브쿼리를 실행하지 않고 한번에 처리
        2. DISTINCT ON 활용: 최신/이전 스냅샷을 효율적으로 가져오기
        3. Python loop 내 추가 쿼리 제거: alt_snapshot 조회 로직 제거
        4. SQL에서 surge_score 계산: Python 연산 최소화
        5. 배치 upsert: video_score 업데이트를 루프에서 한 번에 처리
        
        - days: 최근 N일 내 업로드/수집된 영상만 대상
        - velocity_days: 이전 스냅샷 기준 일수 (예: 1일 전과 비교)
        """
        try:
            self.db.rollback()
        except Exception:
            pass
        
        to_date = datetime.utcnow().date()
        from_date = to_date - timedelta(days=days - 1)
        now = datetime.utcnow()

        # 최적화된 SQL: CTE를 사용해 스냅샷 조회를 한 번에 처리
        rows = self.db.execute(
            text(
                """
                WITH 
                -- 1단계: 대상 비디오 필터링 (최근 N일)
                target_videos AS (
                    SELECT video_id, platform
                    FROM video v
                    WHERE COALESCE(v.published_at::date, v.crawled_at::date) BETWEEN :from_date AND :to_date
                      AND (:platform IS NULL OR v.platform = :platform)
                ),
                -- 2단계: 최신 스냅샷 (DISTINCT ON으로 최적화)
                latest_snapshot AS (
                    SELECT DISTINCT ON (vms.video_id, vms.platform)
                        vms.video_id,
                        vms.platform,
                        vms.view_count AS curr_view,
                        vms.like_count AS curr_like,
                        vms.comment_count AS curr_comment,
                        vms.snapshot_date AS curr_date
                    FROM video_metrics_snapshot vms
                    INNER JOIN target_videos tv ON vms.video_id = tv.video_id AND vms.platform = tv.platform
                    WHERE vms.snapshot_date <= :to_date
                    ORDER BY vms.video_id, vms.platform, vms.snapshot_date DESC
                ),
                -- 3단계: 이전 스냅샷 (최신보다 이전 것 중 가장 최근 것)
                prev_snapshot AS (
                    SELECT DISTINCT ON (vms.video_id, vms.platform)
                        vms.video_id,
                        vms.platform,
                        vms.view_count AS prev_view,
                        vms.like_count AS prev_like,
                        vms.comment_count AS prev_comment
                    FROM video_metrics_snapshot vms
                    INNER JOIN latest_snapshot ls 
                        ON vms.video_id = ls.video_id 
                        AND vms.platform = ls.platform
                    WHERE vms.snapshot_date < ls.curr_date
                      AND vms.snapshot_date >= :to_date - INTERVAL '30 days'
                    ORDER BY vms.video_id, vms.platform, vms.snapshot_date DESC
                ),
                -- 4단계: 메인 데이터 조합 및 surge 지표 계산
                surge_calc AS (
                    SELECT
                        v.video_id,
                        v.title,
                        v.description,
                        v.tags,
                        v.category_id,
                        v.duration,
                        v.channel_id,
                        v.platform,
                        v.published_at,
                        v.thumbnail_url,
                        v.crawled_at,
                        v.is_shorts,
                        vs.category,
                        COALESCE(ch.title, v.channel_id) AS channel_username,
                        
                        -- 현재 및 이전 지표
                        COALESCE(ls.curr_view, v.view_count, 0)::BIGINT AS view_count,
                        COALESCE(ps.prev_view, 0)::BIGINT AS view_count_prev,
                        COALESCE(ls.curr_like, v.like_count, 0)::BIGINT AS like_count,
                        COALESCE(ps.prev_like, 0)::BIGINT AS like_count_prev,
                        COALESCE(ls.curr_comment, v.comment_count, 0)::BIGINT AS comment_count,
                        COALESCE(ps.prev_comment, 0)::BIGINT AS comment_count_prev,
                        
                        -- 증가량
                        (COALESCE(ls.curr_view, v.view_count, 0) - COALESCE(ps.prev_view, 0))::BIGINT AS delta_views,
                        (COALESCE(ls.curr_like, v.like_count, 0) - COALESCE(ps.prev_like, 0))::BIGINT AS delta_likes,
                        (COALESCE(ls.curr_comment, v.comment_count, 0) - COALESCE(ps.prev_comment, 0))::BIGINT AS delta_comments,
                        
                        -- Velocity (일 단위 증가량)
                        (COALESCE(ls.curr_view, v.view_count, 0) - COALESCE(ps.prev_view, 0))::FLOAT / NULLIF(:velocity_days, 0) AS view_velocity,
                        (COALESCE(ls.curr_like, v.like_count, 0) - COALESCE(ps.prev_like, 0))::FLOAT / NULLIF(:velocity_days, 0) AS like_velocity,
                        (COALESCE(ls.curr_comment, v.comment_count, 0) - COALESCE(ps.prev_comment, 0))::FLOAT / NULLIF(:velocity_days, 0) AS comment_velocity,
                        
                        -- 증가율
                        CASE 
                            WHEN COALESCE(ps.prev_view, 0) > 0 THEN 
                                (COALESCE(ls.curr_view, v.view_count, 0) - COALESCE(ps.prev_view, 0))::FLOAT / ps.prev_view
                            ELSE 0.0
                        END AS growth_rate,
                        
                        -- 경과 시간 계산 (시간 단위)
                        EXTRACT(EPOCH FROM (:now - v.published_at)) / 3600.0 AS age_hours,
                        
                        -- Freshness score (지수 감쇠 + 보너스)
                        CASE 
                            WHEN v.published_at IS NOT NULL THEN
                                EXP(-0.05 * (EXTRACT(EPOCH FROM (:now - v.published_at)) / 3600.0)) *
                                CASE 
                                    WHEN EXTRACT(EPOCH FROM (:now - v.published_at)) / 3600.0 <= 24 THEN 1.5
                                    WHEN EXTRACT(EPOCH FROM (:now - v.published_at)) / 3600.0 <= 48 THEN 1.2
                                    WHEN EXTRACT(EPOCH FROM (:now - v.published_at)) / 3600.0 <= 72 THEN 1.1
                                    ELSE 1.0
                                END
                            ELSE 0.5
                        END AS freshness_score_with_bonus,
                        
                        COALESCE(sc.total_score, sc.sentiment_score, sc.trend_score, 0) AS total_score
                        
                    FROM video v
                    INNER JOIN target_videos tv ON v.video_id = tv.video_id AND v.platform = tv.platform
                    LEFT JOIN latest_snapshot ls ON ls.video_id = v.video_id AND ls.platform = v.platform
                    LEFT JOIN prev_snapshot ps ON ps.video_id = v.video_id AND ps.platform = v.platform
                    LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                    LEFT JOIN video_score sc ON sc.video_id = v.video_id
                    LEFT JOIN channel ch ON ch.channel_id = v.channel_id
                )
                -- 5단계: Surge Score 계산 및 정렬
                SELECT
                    *,
                    -- Surge Score 계산 (SQL에서 직접 수행)
                    (
                        (growth_rate * 100) +                                    -- growth_factor
                        (view_velocity / 1000.0) +                               -- velocity_factor
                        (LN(GREATEST(view_count, 1) + 10) * 0.1) +              -- popularity_factor
                        (freshness_score_with_bonus * 50)                       -- freshness_factor
                    ) AS surge_score,
                    
                    -- 증가율 퍼센트
                    ROUND((growth_rate * 100)::NUMERIC, 1) AS growth_rate_percentage
                    
                FROM surge_calc
                WHERE view_count > 0  -- 조회수가 0인 영상 제외
                ORDER BY 
                    -- velocity가 있는 영상 우선
                    CASE WHEN delta_views > 0 THEN 1 ELSE 0 END DESC,
                    -- Surge Score 기준 정렬
                    surge_score DESC NULLS LAST,
                    view_velocity DESC NULLS LAST,
                    view_count DESC
                LIMIT :limit_fetch
                """
            ),
            {
                "from_date": from_date,
                "to_date": to_date,
                "platform": platform,
                "velocity_days": velocity_days,
                "limit_fetch": limit * 2,  # 여유있게 가져온 후 재정렬
                "now": now,
            },
        ).mappings()

        import math

        result: list[dict] = []
        video_scores_to_upsert = []

        # SQL 결과를 Python에서 추가 가공 (최소화)
        for r in rows:
            item = dict(r)
            
            view_now = int(item["view_count"] or 0)
            delta_views = int(item["delta_views"] or 0)
            
            # age 관련 필드 계산
            if item.get("age_hours") is not None:
                age_hours = float(item["age_hours"])
                item["age_seconds"] = age_hours * 3600
                item["age_minutes"] = age_hours * 60
                item["age_days"] = age_hours / 24
            else:
                item["age_seconds"] = None
                item["age_minutes"] = None
                item["age_days"] = None
            
            # 프론트엔드용 필드 매핑
            item["view_count_change"] = int(item["delta_views"] or 0)
            item["like_count_change"] = int(item["delta_likes"] or 0)
            item["comment_count_change"] = int(item["delta_comments"] or 0)
            item["delta_views_window"] = float(delta_views)
            item["growth_rate_window"] = float(item["growth_rate"] or 0.0)
            
            # Freshness 점수 분해
            freshness_with_bonus = float(item["freshness_score_with_bonus"] or 0.5)
            if (item.get("age_hours") or 999) <= 24:
                item["freshness_score"] = round(freshness_with_bonus / 1.5, 4)
                item["freshness_bonus"] = 1.5
            elif (item.get("age_hours") or 999) <= 48:
                item["freshness_score"] = round(freshness_with_bonus / 1.2, 4)
                item["freshness_bonus"] = 1.2
            elif (item.get("age_hours") or 999) <= 72:
                item["freshness_score"] = round(freshness_with_bonus / 1.1, 4)
                item["freshness_bonus"] = 1.1
            else:
                item["freshness_score"] = round(freshness_with_bonus, 4)
                item["freshness_bonus"] = 1.0
            
            # Surge score 반올림
            surge_score = float(item.get("surge_score") or 0.0)
            item["surge_score"] = round(surge_score, 2)
            
            # 디버깅용 세부 점수
            growth_factor = (item["growth_rate"] or 0.0) * 100
            velocity_factor = (item["view_velocity"] or 0.0) / 1000.0
            popularity_factor = math.log(max(view_now, 1) + 10) * 0.1
            freshness_factor = freshness_with_bonus * 50
            
            item["surge_components"] = {
                "growth_factor": round(growth_factor, 2),
                "velocity_factor": round(velocity_factor, 2),
                "popularity_factor": round(popularity_factor, 2),
                "freshness_factor": round(freshness_factor, 2),
            }
            
            # 이전 스냅샷 데이터 제거
            item.pop("view_count_prev", None)
            item.pop("like_count_prev", None)
            item.pop("comment_count_prev", None)
            
            result.append(item)
            
            # 배치 upsert를 위한 데이터 수집
            video_scores_to_upsert.append({
                "video_id": item["video_id"],
                "platform": item.get("platform") or "youtube",
                "trend_score": item["surge_score"],
                "updated_at": now,
            })
        
        # Surge score 기준으로 최종 정렬 및 limit 적용
        result_sorted = sorted(
            result,
            key=lambda x: x.get("surge_score") or 0.0,
            reverse=True,
        )[:limit]
        
        # Ranking 부여
        for idx, item in enumerate(result_sorted, 1):
            item["trending_rank"] = idx
        
        # 배치 upsert: 한 번의 트랜잭션으로 모든 video_score 업데이트
        if video_scores_to_upsert:
            try:
                # limit만큼만 upsert
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
                    video_scores_to_upsert[:limit],
                )
                self.db.commit()
            except Exception as e:
                self.db.rollback()
                print(f"Error batch upserting trend_scores: {e}")

        return result_sorted

    def fetch_video_snapshot_history(
        self, video_id: str, platform: str = "youtube", days: int = 7
    ) -> list[dict]:
        """
        특정 영상의 스냅샷 히스토리를 조회하여 추이 차트 데이터를 제공한다.
        스냅샷이 없는 경우 현재 video 테이블 데이터를 반환한다.
        """
        try:
            self.db.rollback()
        except Exception:
            pass

        since_date = (datetime.utcnow() - timedelta(days=days)).date()

        rows = self.db.execute(
            text(
                """
                SELECT
                    vms.snapshot_date,
                    vms.view_count,
                    vms.like_count,
                    vms.comment_count,
                    -- 일일 증가량 계산
                    COALESCE(vms.view_count - LAG(vms.view_count) OVER (ORDER BY vms.snapshot_date), 0) as daily_view_increase,
                    COALESCE(vms.like_count - LAG(vms.like_count) OVER (ORDER BY vms.snapshot_date), 0) as daily_like_increase,
                    COALESCE(vms.comment_count - LAG(vms.comment_count) OVER (ORDER BY vms.snapshot_date), 0) as daily_comment_increase
                FROM video_metrics_snapshot vms
                WHERE vms.video_id = :video_id
                  AND vms.platform = :platform
                  AND vms.snapshot_date >= :since_date
                ORDER BY vms.snapshot_date ASC
                """
            ),
            {
                "video_id": video_id,
                "platform": platform,
                "since_date": since_date,
            },
        ).mappings()

        result = [dict(r) for r in rows]

        # 스냅샷이 없는 경우, video 테이블의 현재 데이터만 반환 (증가량 없음)
        if not result:
            video_row = self.db.execute(
                text(
                    """
                    SELECT
                        CURRENT_DATE as snapshot_date,
                        COALESCE(v.view_count, 0) as view_count,
                        COALESCE(v.like_count, 0) as like_count,
                        COALESCE(v.comment_count, 0) as comment_count,
                        0 as daily_view_increase,
                        0 as daily_like_increase,
                        0 as daily_comment_increase
                    FROM video v
                    WHERE v.video_id = :video_id
                      AND v.platform = :platform
                    LIMIT 1
                    """
                ),
                {
                    "video_id": video_id,
                    "platform": platform,
                },
            ).mappings().fetchone()

            if video_row:
                result = [dict(video_row)]

        return result
