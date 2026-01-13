from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class TranscriptSegment:
    start: float
    end: float
    text: str


@dataclass
class DetectedObject:
    class_name: str
    confidence: float


@dataclass
class VisualFrame:
    timestamp: float
    objects: List[DetectedObject]


@dataclass
class VideoAnalysisResult:
    video_id: str
    transcript_text: str
    transcript_segments: List[TranscriptSegment]
    visual_frames: List[VisualFrame]
    subtitle_data: Optional[Dict]
    analyzed_at: datetime
    video_title: Optional[str] = None
    video_duration: Optional[str] = None  # ISO 8601 형식 (PT1M30S) 또는 초 단위