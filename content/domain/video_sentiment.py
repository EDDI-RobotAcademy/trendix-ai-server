from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class VideoSentiment:
    video_id: str
    platform: str | None = None
    category: Optional[str] = None
    trend_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    sentiment_score: Optional[float] = None
    keywords: Optional[str] = None
    summary: Optional[str] = None
    analyzed_at: Optional[datetime] = None
