from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Video:
    video_id: str
    channel_id: str
    title: str
    platform: str | None = None
    description: Optional[str] = None
    tags: Optional[str] = None
    category_id: Optional[int] = None
    published_at: Optional[datetime] = None
    duration: Optional[str] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    thumbnail_url: Optional[str] = None
    crawled_at: Optional[datetime] = None
    is_shorts: Optional[bool] = None

    @classmethod
    def from_platform(cls, payload: dict) -> "Video":
        return cls(
            video_id=payload.get("video_id") or payload.get("id"),
            channel_id=payload.get("channel_id"),
            title=payload.get("title", ""),
            platform=payload.get("platform"),
            description=payload.get("description"),
            tags=payload.get("tags"),
            category_id=payload.get("category_id"),
            published_at=payload.get("published_at"),
            duration=payload.get("duration"),
            view_count=payload.get("view_count"),
            like_count=payload.get("like_count"),
            comment_count=payload.get("comment_count"),
            thumbnail_url=payload.get("thumbnail_url"),
            crawled_at=payload.get("crawled_at"),
            is_shorts=payload.get("is_shorts"),
        )
