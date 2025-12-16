from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class VideoComment:
    comment_id: str
    video_id: str
    platform: str | None
    author: Optional[str]
    content: str
    like_count: Optional[int]
    published_at: Optional[datetime]

    @classmethod
    def from_platform(cls, payload: dict) -> "VideoComment":
        return cls(
            comment_id=payload.get("comment_id") or payload.get("id"),
            video_id=payload.get("video_id"),
            platform=payload.get("platform"),
            author=payload.get("author"),
            content=payload.get("content", ""),
            like_count=payload.get("like_count"),
            published_at=payload.get("published_at"),
        )
