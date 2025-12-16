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
    def fetch_recommended_videos_by_category(
        self, category: str, limit: int = 20, days: int = 14, platform: str | None = None
    ) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_distinct_categories(self, limit: int = 100) -> list[str]:
        raise NotImplementedError
