from typing import Iterable

from content.application.port.platform_client_port import PlatformClientPort
from content.domain.channel import Channel
from content.domain.video import Video
from content.domain.video_comment import VideoComment


class TikTokClient(PlatformClientPort):
    platform = "tiktok"

    def __init__(self, base_url: str = "", api_key: str = ""):
        self.base_url = base_url
        self.api_key = api_key

    def fetch_channel(self, channel_id: str) -> Channel:
        raise NotImplementedError("TikTok API integration pending")

    def fetch_videos(self, channel_id: str, max_results: int = 20) -> Iterable[Video]:
        raise NotImplementedError("TikTok API integration pending")

    def fetch_video(self, video_id: str) -> Video:
        raise NotImplementedError("TikTok API integration pending")

    def fetch_comments(self, video_id: str, max_results: int = 50) -> Iterable[VideoComment]:
        raise NotImplementedError("TikTok API integration pending")