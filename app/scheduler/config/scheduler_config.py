import os
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from enum import Enum


class CollectionStrategy(Enum):
    """수집 전략 타입"""
    TRENDING_VIDEOS = "trending_videos"
    SELECTIVE_VIDEOS = "selective_videos" 
    CATEGORY_BASED = "category_based"


class TrendAnalysisMethod(Enum):
    """트렌드 분석 방법"""
    GROWTH_RATE = "growth_rate"
    VELOCITY_BASED = "velocity_based"
    COMPOSITE_SCORE = "composite_score"


@dataclass
class CollectionConfig:
    """영상 수집 설정"""
    max_videos_per_cycle: int = 50
    min_view_count: int = 1000
    min_like_count: int = 10
    max_video_age_hours: int = 24
    target_categories: List[int] = field(default_factory=lambda: [10, 24, 25])  # 음악, 엔터테인먼트, 뉴스
    exclude_channels: List[str] = field(default_factory=list)
    include_shorts_only: bool = True
    
    def __post_init__(self):
        if self.max_videos_per_cycle <= 0:
            raise ValueError("max_videos_per_cycle must be positive")
        if self.min_view_count < 0:
            raise ValueError("min_view_count cannot be negative")


@dataclass  
class TrendAnalysisConfig:
    """트렌드 분석 설정"""
    method: TrendAnalysisMethod = TrendAnalysisMethod.COMPOSITE_SCORE
    view_growth_weight: float = 0.4
    like_growth_weight: float = 0.3
    comment_growth_weight: float = 0.2
    velocity_weight: float = 0.1
    min_trend_score: float = 0.6
    analysis_window_hours: int = 8
    
    def __post_init__(self):
        total_weight = (
            self.view_growth_weight + 
            self.like_growth_weight + 
            self.comment_growth_weight + 
            self.velocity_weight
        )
        if abs(total_weight - 1.0) > 0.01:
            raise ValueError("Weight sum must equal 1.0")


@dataclass
class SchedulerConfig:
    """스케줄러 메인 설정"""
    scheduler_id: str
    interval_minutes: int = 30
    collection_strategy: CollectionStrategy = CollectionStrategy.SELECTIVE_VIDEOS
    collection_config: CollectionConfig = field(default_factory=CollectionConfig)
    analysis_config: TrendAnalysisConfig = field(default_factory=TrendAnalysisConfig) 
    enabled: bool = True
    retry_count: int = 3
    retry_delay_seconds: int = 60
    max_execution_time_minutes: int = 10
    
    def __post_init__(self):
        if self.interval_minutes <= 0:
            raise ValueError("interval_minutes must be positive")
        if not self.scheduler_id:
            raise ValueError("scheduler_id is required")
        if self.retry_count < 0:
            raise ValueError("retry_count cannot be negative")


class SchedulerConfigFactory:
    """스케줄러 설정 팩토리"""
    
    @classmethod
    def create_from_env(cls, scheduler_id: str) -> SchedulerConfig:
        """환경변수로부터 설정 생성"""
        return SchedulerConfig(
            scheduler_id=scheduler_id,
            interval_minutes=int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "30")),
            collection_strategy=CollectionStrategy(
                os.getenv("COLLECTION_STRATEGY", "selective_videos")
            ),
            collection_config=cls._create_collection_config(),
            analysis_config=cls._create_analysis_config(),
            enabled=os.getenv("SCHEDULER_ENABLED", "true").lower() == "true",
            retry_count=int(os.getenv("SCHEDULER_RETRY_COUNT", "3")),
            retry_delay_seconds=int(os.getenv("SCHEDULER_RETRY_DELAY", "60")),
            max_execution_time_minutes=int(os.getenv("SCHEDULER_MAX_EXECUTION_TIME", "10"))
        )
    
    @classmethod
    def create_mvp_config(cls, scheduler_id: str) -> SchedulerConfig:
        """MVP 요구사항에 맞는 설정 생성"""
        mvp_collection_config = CollectionConfig(
            max_videos_per_cycle=30,  # MVP: 선별된 영상만
            min_view_count=500,   # 급등 탐지를 위해 낮춤
            min_like_count=5,     # 급등 탐지를 위해 낮춤
            max_video_age_hours=8,  # MVP 요구사항: 8시간 기준
            target_categories=[10, 22, 24, 25],  # 음악, 인물/블로그, 엔터테인먼트, 뉴스
            include_shorts_only=False  # 전체 영상 대상 (Shorts 구분은 별도 컬럼)
        )
        
        mvp_analysis_config = TrendAnalysisConfig(
            method=TrendAnalysisMethod.COMPOSITE_SCORE,
            view_growth_weight=0.5,  # 조회수 증가에 더 높은 가중치
            like_growth_weight=0.3,
            comment_growth_weight=0.1,
            velocity_weight=0.1,
            min_trend_score=0.7,  # 더 엄격한 기준
            analysis_window_hours=8
        )
        
        return SchedulerConfig(
            scheduler_id=scheduler_id,
            interval_minutes=10,  # MVP 요구사항: 10분 주기
            collection_strategy=CollectionStrategy.SELECTIVE_VIDEOS,
            collection_config=mvp_collection_config,
            analysis_config=mvp_analysis_config,
            enabled=True,
            retry_count=3,
            retry_delay_seconds=60,
            max_execution_time_minutes=15  # 10분 주기에 맞춰 실행시간 단축
        )
    
    @classmethod
    def _create_collection_config(cls) -> CollectionConfig:
        """환경변수로부터 수집 설정 생성"""
        target_categories = []
        if categories_str := os.getenv("TARGET_CATEGORIES"):
            target_categories = [int(x.strip()) for x in categories_str.split(",")]
            
        exclude_channels = []
        if channels_str := os.getenv("EXCLUDE_CHANNELS"):
            exclude_channels = [x.strip() for x in channels_str.split(",")]
        
        return CollectionConfig(
            max_videos_per_cycle=int(os.getenv("MAX_VIDEOS_PER_CYCLE", "50")),
            min_view_count=int(os.getenv("MIN_VIEW_COUNT", "1000")),
            min_like_count=int(os.getenv("MIN_LIKE_COUNT", "10")),
            max_video_age_hours=int(os.getenv("MAX_VIDEO_AGE_HOURS", "24")),
            target_categories=target_categories or [10, 24, 25],
            exclude_channels=exclude_channels,
            include_shorts_only=os.getenv("INCLUDE_SHORTS_ONLY", "true").lower() == "true"
        )
    
    @classmethod
    def _create_analysis_config(cls) -> TrendAnalysisConfig:
        """환경변수로부터 분석 설정 생성"""
        return TrendAnalysisConfig(
            method=TrendAnalysisMethod(os.getenv("ANALYSIS_METHOD", "composite_score")),
            view_growth_weight=float(os.getenv("VIEW_GROWTH_WEIGHT", "0.4")),
            like_growth_weight=float(os.getenv("LIKE_GROWTH_WEIGHT", "0.3")),
            comment_growth_weight=float(os.getenv("COMMENT_GROWTH_WEIGHT", "0.2")),
            velocity_weight=float(os.getenv("VELOCITY_WEIGHT", "0.1")),
            min_trend_score=float(os.getenv("MIN_TREND_SCORE", "0.6")),
            analysis_window_hours=int(os.getenv("ANALYSIS_WINDOW_HOURS", "8"))
        )