from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from content.domain.video import Video


class SchedulerStatus(Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"


@dataclass
class CollectionResult:
    """영상 수집 결과"""
    videos: List[Video]
    total_collected: int
    filtered_count: int
    execution_time: float
    timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class TrendAnalysisResult:
    """트렌드 분석 결과"""
    trending_videos: List[Video]
    analysis_metrics: Dict[str, float]
    category_trends: Dict[str, List[Video]]
    timestamp: datetime


class VideoCollectorInterface(ABC):
    """영상 수집 전략 인터페이스"""
    
    @abstractmethod
    async def collect_videos(self, max_videos: int = 50) -> CollectionResult:
        """영상 수집"""
        pass
    
    @abstractmethod
    def should_collect_video(self, video: Video) -> bool:
        """영상 수집 여부 판단"""
        pass


class TrendAnalyzerInterface(ABC):
    """트렌드 분석 인터페이스"""
    
    @abstractmethod
    async def analyze_trends(self, videos: List[Video]) -> TrendAnalysisResult:
        """트렌드 분석 수행"""
        pass
    
    @abstractmethod
    def calculate_trend_score(self, video: Video) -> float:
        """트렌드 점수 계산"""
        pass


class SchedulerInterface(ABC):
    """스케줄러 인터페이스"""
    
    @abstractmethod
    async def start(self) -> None:
        """스케줄러 시작"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """스케줄러 정지"""
        pass
    
    @abstractmethod
    async def pause(self) -> None:
        """스케줄러 일시 정지"""
        pass
    
    @abstractmethod
    async def resume(self) -> None:
        """스케줄러 재개"""
        pass
    
    @abstractmethod
    def get_status(self) -> SchedulerStatus:
        """현재 상태 반환"""
        pass


class SchedulerObserverInterface(ABC):
    """스케줄러 이벤트 관찰자 인터페이스"""
    
    @abstractmethod
    async def on_collection_completed(self, result: CollectionResult) -> None:
        """영상 수집 완료 이벤트"""
        pass
    
    @abstractmethod
    async def on_analysis_completed(self, result: TrendAnalysisResult) -> None:
        """트렌드 분석 완료 이벤트"""
        pass
    
    @abstractmethod
    async def on_error(self, error: Exception) -> None:
        """에러 발생 이벤트"""
        pass