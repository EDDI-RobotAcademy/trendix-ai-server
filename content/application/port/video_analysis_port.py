from abc import ABC, abstractmethod

from content.domain.video_analysis import VideoAnalysisResult


class VideoAnalysisPort(ABC):
    @abstractmethod
    async def analyze_video(self, video_id: str, video_url: str) -> VideoAnalysisResult:
        pass