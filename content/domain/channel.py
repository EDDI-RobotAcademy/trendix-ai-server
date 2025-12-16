from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Channel:
    channel_id: str
    title: str
    platform: str | None = None
    description: Optional[str] = None
    country: Optional[str] = None
    subscriber_count: Optional[int] = None
    view_count: Optional[int] = None
    video_count: Optional[int] = None
    created_at: Optional[datetime] = None
    crawled_at: Optional[datetime] = None

    @classmethod
    def from_platform(cls, payload: dict) -> "Channel":
        return cls(
            channel_id=payload.get("channel_id") or payload.get("id"),
            title=payload.get("title", ""),
            platform=payload.get("platform"),
            description=payload.get("description"),
            country=payload.get("country"),
            subscriber_count=payload.get("subscriber_count"),
            view_count=payload.get("view_count"),
            video_count=payload.get("video_count"),
            created_at=payload.get("created_at"),
            crawled_at=payload.get("crawled_at"),
        )
