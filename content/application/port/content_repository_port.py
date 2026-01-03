from abc import ABC, abstractmethod
from typing import Iterable

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


class ContentRepositoryPort(ABC):
    @abstractmethod
    def upsert_channel(self, channel: Channel) -> Channel:
        raise NotImplementedError

    @abstractmethod
    def upsert_account(self, account: CreatorAccount) -> CreatorAccount:
        raise NotImplementedError

    @abstractmethod
    def upsert_video(self, video: Video) -> Video:
        raise NotImplementedError

    @abstractmethod
    def upsert_comments(self, comments: Iterable[VideoComment]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_video_sentiment(self, sentiment: VideoSentiment) -> VideoSentiment:
        raise NotImplementedError

    @abstractmethod
    def upsert_comment_sentiments(self, sentiments: Iterable[CommentSentiment]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_keyword_trend(self, trend: KeywordTrend) -> KeywordTrend:
        raise NotImplementedError

    @abstractmethod
    def upsert_category_trend(self, trend: CategoryTrend) -> CategoryTrend:
        raise NotImplementedError

    @abstractmethod
    def upsert_keyword_mapping(self, mapping: KeywordMapping) -> KeywordMapping:
        raise NotImplementedError

    @abstractmethod
    def upsert_video_score(self, score: VideoScore) -> VideoScore:
        raise NotImplementedError

    @abstractmethod
    def log_crawl(self, log: CrawlLog) -> CrawlLog:
        raise NotImplementedError

    @abstractmethod
    def upsert_video_metrics_snapshot(self, snapshot: VideoMetricsSnapshot) -> None:
        raise NotImplementedError

    # 조회 전용 메서드들
    @abstractmethod
    def fetch_videos_by_category(self, category: str, limit: int = 20) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_videos_by_keyword(self, keyword: str, limit: int = 20) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_top_keywords_by_category(self, category: str, limit: int = 10) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_top_keywords_by_keyword(self, keyword: str, limit: int = 10) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_video_with_scores(self, video_id: str) -> dict | None:
        raise NotImplementedError

    @abstractmethod
    def fetch_hot_category_trends(self, platform: str | None = None, limit: int = 20) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_popular_videos(self, limit: int = 5, platform: str | None = None) -> list[dict]:
        """절대 인기 상위"""
        raise NotImplementedError

    @abstractmethod
    def fetch_rising_videos(self, limit: int = 5, velocity_days: int = 1, platform: str | None = None) -> list[dict]:
        """최근 증가량/가속도 기반 상위"""
        raise NotImplementedError

    @abstractmethod
    def fetch_recommended_videos_by_category(
        self, category: str, limit: int = 20, days: int = 14, platform: str | None = None
    ) -> list[dict]:
        """
        카테고리 문자열(category) 기준으로 최근 N일 내 추천 영상을 조회한다.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_distinct_categories(self, limit: int = 100) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def fetch_surge_videos(
        self,
        platform: str | None = None,
        limit: int = 30,
        days: int = 3,
        velocity_days: int = 1,
    ) -> list[dict]:
        """
        단기 조회수 증가량/증가율을 기준으로 급등 영상 랭킹 리스트를 조회한다.

        - days: 최근 N일 내 업로드/수집된 영상만 대상
        - velocity_days: 이전 스냅샷 기준 일수 (예: 1일 전과 비교)
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_videos_by_category_id(
        self,
        category_id: int,
        limit: int = 10,
        platform: str | None = None,
        days: int | None = None,
    ) -> list[dict]:
        """
        YouTube category_id 기준으로 상위 영상 리스트를 조회한다.
        - days: 최근 N일 내 게시된 영상만 대상 (None이면 전체)
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_video_view_history(
        self,
        video_id: str,
        platform: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        video_metrics_snapshot 기준으로 단일 영상의 일자별(view_count, like_count, comment_count) 히스토리를 조회한다.
        - snapshot_date 내림차순으로 정렬
        """
        raise NotImplementedError