from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class KeywordTrend:
    keyword: str
    date: date
    platform: str
    search_volume: Optional[int] = None
    search_volume_prev: Optional[int] = None
    video_count: Optional[int] = None
    video_count_prev: Optional[int] = None
    avg_sentiment: Optional[float] = None
    avg_trend: Optional[float] = None
    avg_total_score: Optional[float] = None
    growth_rate: Optional[float] = None
    rank: Optional[int] = None
