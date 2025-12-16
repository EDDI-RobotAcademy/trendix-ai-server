from typing import Iterable

from content.application.port.content_repository_port import ContentRepositoryPort
from content.application.port.platform_client_port import PlatformClientPort
from content.domain.creator_account import CreatorAccount
from content.domain.video import Video
from content.domain.video_comment import VideoComment
from content.domain.video_sentiment import VideoSentiment
from content.domain.comment_sentiment import CommentSentiment
from content.domain.crawl_log import CrawlLog
from content.domain.channel import Channel
from content.domain.video_score import VideoScore
from content.domain.keyword_mapping import KeywordMapping


class IngestionUseCase:
    def __init__(self, repository: ContentRepositoryPort, sentiment_usecase=None):
        # 한국어 주석: 저장소와 감정 분석 모듈을 주입받아 플랫폼 무관하게 전 범위 콘텐츠를 적재합니다.
        self.repository = repository
        self.sentiment_usecase = sentiment_usecase

    def ingest_channel_bundle(
        self,
        client: PlatformClientPort,
        channel_id: str,
        include_comments: bool = False,
        max_videos: int = 10,
        max_comments: int = 50,
    ) -> dict:
        # 한국어 주석: 채널, 영상, 댓글까지 가능한 모든 데이터를 모아 후속 분류·추천에 쓰도록 합니다.
        channel = client.fetch_channel(channel_id)
        channel.platform = client.platform
        # 한국어 주석: 계정/채널 단위 정보를 별도 테이블에 적재하여 팔로워/게시물 등 변동성 필드만 추적합니다.
        self.repository.upsert_account(
            CreatorAccount(
                account_id=channel.channel_id,
                platform=client.platform,
                display_name=channel.title,
                description=channel.description,
                country=channel.country,
                follower_count=channel.subscriber_count,
                post_count=channel.video_count,
                last_updated_at=channel.crawled_at,
                crawled_at=channel.crawled_at,
            )
        )
        self.repository.upsert_channel(channel)

        videos = list(client.fetch_videos(channel_id, max_results=max_videos))
        ingested_videos: list[str] = []
        ingested_comments: int = 0

        for video in videos:
            video.platform = client.platform
            self._persist_video(video)
            ingested_videos.append(video.video_id)

            if self.sentiment_usecase:
                sentiment = self.sentiment_usecase.analyze_video(video)
                sentiment.platform = client.platform
                self.repository.upsert_video_sentiment(sentiment)
                score = VideoScore(
                    video_id=video.video_id,
                    platform=client.platform,
                    sentiment_score=sentiment.sentiment_score,
                    trend_score=sentiment.trend_score,
                )
                self.repository.upsert_video_score(score)

            if include_comments:
                comments = list(client.fetch_comments(video.video_id, max_results=max_comments))
                for c in comments:
                    c.platform = client.platform
                self.repository.upsert_comments(comments)
                ingested_comments += len(comments)
                if self.sentiment_usecase:
                    sentiments = self.sentiment_usecase.analyze_comments(comments)
                    for s in sentiments:
                        s.platform = client.platform
                    self.repository.upsert_comment_sentiments(sentiments)

        self.repository.log_crawl(
            CrawlLog(
                id=None,
                target_type="channel",
                target_id=channel.channel_id,
                status="success",
                message=f"{len(ingested_videos)} videos, {ingested_comments} comments ingested",
            )
        )

        return {
            "channel_id": channel.channel_id,
            "videos": ingested_videos,
            "comment_count": ingested_comments,
        }

    def ingest_video(
        self,
        client: PlatformClientPort,
        video_id: str,
        include_comments: bool = True,
        max_comments: int = 50,
    ) -> dict:
        # 한국어 주석: 단일 영상의 본문·태그·댓글 등 전체 정보를 수집해 분석과 추천의 기반을 만듭니다.
        video = client.fetch_video(video_id)
        video.platform = client.platform
        self._persist_video(video)

        comments: list[VideoComment] = []
        if include_comments:
            comments = list(client.fetch_comments(video_id, max_results=max_comments))
            for c in comments:
                c.platform = client.platform
            self.repository.upsert_comments(comments)
            if self.sentiment_usecase:
                sentiments = self.sentiment_usecase.analyze_comments(comments)
                for s in sentiments:
                    s.platform = client.platform
                self.repository.upsert_comment_sentiments(sentiments)

        video_sentiment = None
        if self.sentiment_usecase:
            video_sentiment = self.sentiment_usecase.analyze_video(video)
            video_sentiment.platform = client.platform
            self.repository.upsert_video_sentiment(video_sentiment)
            score = VideoScore(
                video_id=video.video_id,
                platform=client.platform,
                sentiment_score=video_sentiment.sentiment_score,
                trend_score=video_sentiment.trend_score,
            )
            self.repository.upsert_video_score(score)

        self.repository.log_crawl(
            CrawlLog(
                id=None,
                target_type="video",
                target_id=video.video_id,
                status="success",
                message=f"{len(comments)} comments ingested",
            )
        )

        return {
            "video_id": video.video_id,
            "comment_count": len(comments),
            "sentiment": video_sentiment.sentiment_label if video_sentiment else None,
        }

    def update_keyword_mapping(self, mappings: Iterable[KeywordMapping]) -> int:
        count = 0
        for mapping in mappings:
            self.repository.upsert_keyword_mapping(mapping)
            count += 1
        return count

    def _persist_video(self, video: Video):
        self.repository.upsert_video(video)
        if not video.tags:
            return
        keywords = [tag.strip() for tag in video.tags.split(",") if tag.strip()]
        for kw in keywords:
            self.repository.upsert_keyword_mapping(
                KeywordMapping(
                    mapping_id=None,
                    video_id=video.video_id,
                    channel_id=video.channel_id,
                    platform=video.platform,
                    keyword=kw,
                    weight=1.0,
                )
            )
