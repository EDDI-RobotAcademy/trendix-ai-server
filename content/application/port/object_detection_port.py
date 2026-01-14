from abc import ABC, abstractmethod
from typing import List

from content.domain.video_analysis import VisualFrame


class ObjectDetectionPort(ABC):
    @abstractmethod
    async def detect_objects(self, video_path: str) -> List[VisualFrame]:
        pass