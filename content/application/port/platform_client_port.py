from abc import ABC, abstractmethod
from typing import Iterable

from content.domain.channel import Channel
from content.domain.video import Video
from content.domain.video_comment import VideoComment


class PlatformClientPort(ABC):
    platform: str

    @abstractmethod
    def fetch_channel(self, channel_id: str) -> Channel:
        raise NotImplementedError

    @abstractmethod
    def fetch_videos(self, channel_id: str, max_results: int = 20) -> Iterable[Video]:
        raise NotImplementedError

    @abstractmethod
    def fetch_video(self, video_id: str) -> Video:
        raise NotImplementedError

    @abstractmethod
    def fetch_comments(self, video_id: str, max_results: int = 50) -> Iterable[VideoComment]:
        raise NotImplementedError
