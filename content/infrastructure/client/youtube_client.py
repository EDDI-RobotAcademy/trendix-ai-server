from datetime import datetime
from typing import Iterable, List
from urllib.parse import urlparse

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import YouTubeSettings
from content.application.port.platform_client_port import PlatformClientPort
from content.domain.channel import Channel
from content.domain.video import Video
from content.domain.video_comment import VideoComment


class YouTubeClient(PlatformClientPort):
    platform = "youtube"

    def __init__(self, settings: YouTubeSettings):
        # YouTube Data API? ??/??/??? ????.
        self.settings = settings
        self.service = build(
            "youtube",
            "v3",
            developerKey=settings.api_key,
            cache_discovery=False,
        )

    def fetch_channel(self, channel_id: str) -> Channel:
        resolved_id = self._resolve_channel_id(channel_id)
        try:
            response = (
                self.service.channels().list(part="snippet,statistics", id=resolved_id).execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube channel fetch failed: {exc}") from exc

        items = response.get("items", [])
        if not items:
            raise ValueError("Channel not found")
        snippet = items[0]["snippet"]
        stats = items[0]["statistics"]
        return Channel(
            channel_id=resolved_id,
            platform=self.platform,
            title=snippet.get("title", ""),
            description=snippet.get("description"),
            country=snippet.get("country"),
            subscriber_count=int(stats.get("subscriberCount", 0)),
            view_count=int(stats.get("viewCount", 0)),
            video_count=int(stats.get("videoCount", 0)),
            created_at=self._parse_datetime(snippet.get("publishedAt")),
        )

    def fetch_videos(self, channel_id: str, max_results: int = 20) -> Iterable[Video]:
        resolved_id = self._resolve_channel_id(channel_id)
        video_ids = self._list_video_ids(resolved_id, max_results)
        if not video_ids:
            return []
        try:
            response = (
                self.service.videos()
                .list(part="snippet,contentDetails,statistics", id=",".join(video_ids))
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube videos fetch failed: {exc}") from exc

        videos: List[Video] = []
        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            videos.append(
                Video(
                    video_id=item["id"],
                    channel_id=snippet["channelId"],
                    platform=self.platform,
                    title=snippet.get("title", ""),
                    description=snippet.get("description"),
                    tags=",".join(snippet.get("tags", [])) if snippet.get("tags") else None,
                    category_id=int(snippet.get("categoryId")) if snippet.get("categoryId") else None,
                    published_at=self._parse_datetime(snippet.get("publishedAt")),
                    duration=content.get("duration"),
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats.get("likeCount", 0)) if stats.get("likeCount") else 0,
                    comment_count=int(stats.get("commentCount", 0)) if stats.get("commentCount") else 0,
                    thumbnail_url=(snippet.get("thumbnails", {}).get("high") or {}).get("url"),
                )
            )
        return videos

    def fetch_video(self, video_id: str) -> Video:
        videos = list(self.fetch_videos_for_ids([video_id]))
        if not videos:
            raise ValueError("Video not found")
        return videos[0]

    def fetch_videos_for_ids(self, video_ids: List[str]) -> Iterable[Video]:
        if not video_ids:
            return []
        try:
            response = (
                self.service.videos()
                .list(part="snippet,contentDetails,statistics", id=",".join(video_ids))
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube video fetch failed: {exc}") from exc

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            yield Video(
                video_id=item["id"],
                channel_id=snippet["channelId"],
                platform=self.platform,
                title=snippet.get("title", ""),
                description=snippet.get("description"),
                tags=",".join(snippet.get("tags", [])) if snippet.get("tags") else None,
                category_id=int(snippet.get("categoryId")) if snippet.get("categoryId") else None,
                published_at=self._parse_datetime(snippet.get("publishedAt")),
                duration=content.get("duration"),
                view_count=int(stats.get("viewCount", 0)),
                like_count=int(stats.get("likeCount", 0)) if stats.get("likeCount") else 0,
                comment_count=int(stats.get("commentCount", 0)) if stats.get("commentCount") else 0,
                thumbnail_url=(snippet.get("thumbnails", {}).get("high") or {}).get("url"),
            )

    def fetch_comments(self, video_id: str, max_results: int = 50) -> Iterable[VideoComment]:
        try:
            response = (
                self.service.commentThreads()
                .list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(max_results, 100),
                    textFormat="plainText",
                )
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube comments fetch failed: {exc}") from exc

        comments: List[VideoComment] = []
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append(
                VideoComment(
                    comment_id=item["id"],
                    video_id=video_id,
                    platform=self.platform,
                    author=snippet.get("authorDisplayName"),
                    content=snippet.get("textDisplay", ""),
                    like_count=int(snippet.get("likeCount", 0)),
                    published_at=self._parse_datetime(snippet.get("publishedAt")),
                )
            )
        return comments

    def _resolve_channel_id(self, identifier: str) -> str:
        """??(@), ?? URL, ??? ??? ?? channelId(UC...)? ??."""
        if not identifier:
            raise ValueError("Channel identifier is required")
        ident = identifier.strip()
        if ident.startswith("UC"):
            return ident

        parsed = urlparse(ident)
        if parsed.scheme and parsed.netloc:
            path = parsed.path or ""
            if "/channel/" in path:
                return path.split("/channel/", 1)[1].split("/")[0]
            if "/@" in path:
                handle = path.split("/@", 1)[1].split("/")[0]
                ident = f"@{handle}"
            else:
                ident = path.strip("/").split("/")[0] or ident

        if ident.startswith("@"):
            query = ident
        else:
            query = ident

        channel_id = self._search_channel_id(query)
        if not channel_id:
            raise ValueError("Channel not found from identifier")
        return channel_id

    def _search_channel_id(self, query: str) -> str | None:
        try:
            response = (
                self.service.search()
                .list(part="id", type="channel", q=query, maxResults=1)
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube channel search failed: {exc}") from exc

        items = response.get("items", [])
        if not items:
            return None
        return items[0]["id"].get("channelId")

    def _list_video_ids(self, channel_id: str, max_results: int) -> List[str]:
        try:
            response = (
                self.service.search()
                .list(
                    part="id",
                    channelId=channel_id,
                    maxResults=min(max_results, 50),
                    type="video",
                    order="date",
                )
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"YouTube search failed: {exc}") from exc
        ids: List[str] = []
        for item in response.get("items", []):
            if item["id"]["kind"] == "youtube#video":
                ids.append(item["id"]["videoId"])
        return ids

    @staticmethod
    def _parse_datetime(value: str | None):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
