from abc import ABC, abstractmethod
from typing import Optional

from content.domain.video_analysis import VideoAnalysisResult


class VideoRepositoryPort(ABC):
    @abstractmethod
    async def save_analysis(self, result: VideoAnalysisResult) -> None:
        pass

    @abstractmethod
    async def get_analysis(self, video_id: str) -> Optional[VideoAnalysisResult]:
        pass