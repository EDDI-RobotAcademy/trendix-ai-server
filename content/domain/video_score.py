from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class VideoScore:
    video_id: str
    platform: str | None = None
    engagement_score: Optional[float] = None
    sentiment_score: Optional[float] = None
    trend_score: Optional[float] = None
    total_score: Optional[float] = None
    updated_at: Optional[datetime] = None
