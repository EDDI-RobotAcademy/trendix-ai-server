from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CommentSentiment:
    comment_id: str
    platform: str | None = None
    sentiment_label: Optional[str] = None
    sentiment_score: Optional[float] = None
    analyzed_at: Optional[datetime] = None
