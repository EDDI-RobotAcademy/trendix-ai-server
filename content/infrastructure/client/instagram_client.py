from typing import Iterable

from content.application.port.platform_client_port import PlatformClientPort
from content.domain.channel import Channel
from content.domain.video import Video
from content.domain.video_comment import VideoComment


class InstagramClient(PlatformClientPort):
    platform = "instagram"

    def __init__(self, base_url: str = "", access_token: str = "", app_id: str = ""):
        self.base_url = base_url
        self.access_token = access_token
        self.app_id = app_id

    def fetch_channel(self, channel_id: str) -> Channel:
        raise NotImplementedError("Instagram API integration pending")

    def fetch_videos(self, channel_id: str, max_results: int = 20) -> Iterable[Video]:
        raise NotImplementedError("Instagram API integration pending")

    def fetch_video(self, video_id: str) -> Video:
        raise NotImplementedError("Instagram API integration pending")

    def fetch_comments(self, video_id: str, max_results: int = 50) -> Iterable[VideoComment]:
        raise NotImplementedError("Instagram API integration pending")
