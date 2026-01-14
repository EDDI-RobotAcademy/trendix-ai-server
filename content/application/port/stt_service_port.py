from abc import ABC, abstractmethod
from typing import Dict


class STTServicePort(ABC):
    @abstractmethod
    async def transcribe(self, video_path: str) -> Dict:
        pass