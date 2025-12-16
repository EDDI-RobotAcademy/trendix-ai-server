from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class CategoryTrend:
    # 카테고리별 랭킹·트렌드 정보를 담기 위한 도메인 모델
    category: str
    date: date
    platform: str
    video_count: Optional[int] = None
    video_count_prev: Optional[int] = None
    avg_sentiment: Optional[float] = None
    avg_trend: Optional[float] = None
    avg_total_score: Optional[float] = None
    search_volume: Optional[int] = None
    search_volume_prev: Optional[int] = None
    growth_rate: Optional[float] = None
    rank: Optional[int] = None
