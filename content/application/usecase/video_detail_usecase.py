from datetime import datetime
import re

from content.infrastructure.repository.video_detail_repository import VideoDetailRepository
from content.infrastructure.client.youtube_client import YouTubeClient
from content.application.usecase.ingestion_usecase import IngestionUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from config.settings import YouTubeSettings


class VideoDetailUseCase:
    def __init__(self, repository: VideoDetailRepository):
        self.repository = repository

    def get_video_detail(
        self,
        video_id: str,
        platform: str | None = None,
        history_limit: int = 9,
    ) -> dict:
        detail = self.repository.fetch_video_detail(video_id, platform=platform)
        if not detail:
            # 한국어 주석: DB에 없으면 YouTube API로 수집 후 재조회한다.
            self._ingest_video_if_missing(video_id, platform)
            detail = self.repository.fetch_video_detail(video_id, platform=platform)
        if not detail:
            raise ValueError("영상 정보를 찾을 수 없습니다.")

        history_rows = self.repository.fetch_video_history(
            video_id=video_id,
            platform=platform,
            limit=history_limit,
        )
        history_rows_sorted = list(reversed(history_rows))

        view_history = []
        like_history = []
        if history_rows_sorted:
            for row in history_rows_sorted:
                label = self._format_snapshot_label(row.get("snapshot_date"))
                view_history.append({"time": label, "count": int(row.get("view_count") or 0)})
                like_history.append({"time": label, "count": int(row.get("like_count") or 0)})
        else:
            view_history.append({"time": "현재", "count": int(detail.get("view_count") or 0)})
            like_history.append({"time": "현재", "count": int(detail.get("like_count") or 0)})

        tags_raw = detail.get("tags") or ""
        tags = [tag.strip() for tag in tags_raw.split(",") if tag.strip()]

        thumbnail_url = detail.get("thumbnail_url") or ""
        channel_thumbnail = thumbnail_url or "https://picsum.photos/seed/channel-fallback/100/100"

        return {
            "id": detail.get("video_id"),
            "title": detail.get("title") or "",
            "description": detail.get("description") or "",
            "channelName": detail.get("channel_name") or "",
            "channelId": detail.get("channel_id") or "",
            "channelThumbnail": channel_thumbnail,
            "thumbnailUrl": thumbnail_url or channel_thumbnail,
            "viewCount": int(detail.get("view_count") or 0),
            "likeCount": int(detail.get("like_count") or 0),
            "commentCount": int(detail.get("comment_count") or 0),
            "publishedAt": self._format_datetime(detail.get("published_at")),
            "duration": self._format_duration(detail.get("duration")),
            "categoryId": str(detail.get("category_id") or ""),
            "categoryName": detail.get("category_name") or "",
            "tags": tags,
            "isShort": bool(detail.get("is_shorts")),
            "trendingRank": None,
            "trendingReason": None,
            "viewHistory": view_history,
            "likeHistory": like_history,
        }

    @staticmethod
    def _format_datetime(value: datetime | None) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return value.isoformat()

    @staticmethod
    def _format_snapshot_label(value) -> str:
        if value is None:
            return "-"
        if isinstance(value, str):
            return value
        return value.strftime("%Y-%m-%d")

    @staticmethod
    def _format_duration(duration: str | None) -> str:
        # 한국어 주석: ISO 8601 duration(PT#H#M#S)을 사람이 읽기 쉬운 형식으로 변환한다.
        if not duration:
            return "0:00"
        match = re.match(r"PT(?:(\\d+)H)?(?:(\\d+)M)?(?:(\\d+)S)?", duration)
        if not match:
            return duration
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        total_seconds = hours * 3600 + minutes * 60 + seconds
        if total_seconds <= 0:
            return "0:00"
        mm, ss = divmod(total_seconds, 60)
        hh, mm = divmod(mm, 60)
        if hh > 0:
            return f"{hh}:{mm:02d}:{ss:02d}"
        return f"{mm}:{ss:02d}"

    @staticmethod
    def _ingest_video_if_missing(video_id: str, platform: str | None) -> None:
        # 한국어 주석: 비교 요청에서만 사용하는 자동 수집 로직이다.
        if platform and platform.lower() != "youtube":
            return
        client = YouTubeClient(YouTubeSettings())
        ingestion_usecase = IngestionUseCase(ContentRepositoryImpl(), sentiment_usecase=None)
        ingestion_usecase.ingest_video(client, video_id, include_comments=False, max_comments=0)
