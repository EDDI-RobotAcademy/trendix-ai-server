from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List
from enum import Enum

from content.domain.video import Video
from .interfaces import CollectionResult, TrendAnalysisResult


class EventType(Enum):
    SCHEDULER_STARTED = "scheduler_started"
    SCHEDULER_STOPPED = "scheduler_stopped"
    COLLECTION_STARTED = "collection_started" 
    COLLECTION_COMPLETED = "collection_completed"
    ANALYSIS_STARTED = "analysis_started"
    ANALYSIS_COMPLETED = "analysis_completed"
    ERROR_OCCURRED = "error_occurred"


@dataclass
class SchedulerEvent:
    """스케줄러 이벤트 베이스 클래스"""
    timestamp: datetime
    scheduler_id: str
    metadata: Dict[str, Any] = None
    event_type: EventType = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass 
class CollectionStartedEvent(SchedulerEvent):
    """영상 수집 시작 이벤트"""
    target_count: int = 50
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.COLLECTION_STARTED


@dataclass
class CollectionCompletedEvent(SchedulerEvent):
    """영상 수집 완료 이벤트"""
    result: CollectionResult = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.COLLECTION_COMPLETED


@dataclass
class AnalysisStartedEvent(SchedulerEvent):
    """트렌드 분석 시작 이벤트"""
    video_count: int = 0
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.ANALYSIS_STARTED


@dataclass
class AnalysisCompletedEvent(SchedulerEvent):
    """트렌드 분석 완료 이벤트"""
    result: TrendAnalysisResult = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.ANALYSIS_COMPLETED


@dataclass
class ErrorEvent(SchedulerEvent):
    """에러 이벤트"""
    error: Exception = None
    error_context: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.ERROR_OCCURRED