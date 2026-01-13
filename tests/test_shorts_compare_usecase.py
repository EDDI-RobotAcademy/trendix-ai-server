from datetime import datetime, timedelta, timezone

from content.application.usecase.shorts_compare_usecase import ShortsCompareUseCase
from content.utils.youtube_url import parse_youtube_video_id


class DummyRepository:
    def __init__(self, my_video: dict, trend_video: dict):
        self.my_video = my_video
        self.trend_video = trend_video

    def fetch_video_summary(self, video_id: str, platform: str | None = None) -> dict | None:
        if video_id == "my123":
            return self.my_video
        if video_id == "trend456":
            return self.trend_video
        return None


def test_parse_youtube_video_id():
    assert parse_youtube_video_id("https://youtube.com/shorts/abc123") == "abc123"
    assert parse_youtube_video_id("https://youtu.be/xyz789") == "xyz789"
    assert parse_youtube_video_id("https://www.youtube.com/watch?v=vid001") == "vid001"
    assert parse_youtube_video_id("https://youtube.com/embed/vid002") == "vid002"
    assert parse_youtube_video_id("https://example.com") is None


def test_compare_shorts_builds_expected_shape():
    now = datetime.now(tz=timezone.utc)
    my_video = {
        "video_id": "my123",
        "title": "내 쇼츠",
        "channel_id": "channel1",
        "channel_name": "내 채널",
        "platform": "youtube",
        "view_count": 1000,
        "like_count": 80,
        "comment_count": 10,
        "published_at": now - timedelta(hours=3),
        "thumbnail_url": "https://example.com/my.jpg",
        "duration": "PT25S",
    }
    trend_video = {
        "video_id": "trend456",
        "title": "급등 쇼츠",
        "channel_id": "channel2",
        "channel_name": "급등 채널",
        "platform": "youtube",
        "view_count": 5000,
        "like_count": 400,
        "comment_count": 60,
        "published_at": now - timedelta(hours=1),
        "thumbnail_url": "https://example.com/trend.jpg",
        "duration": "PT20S",
    }

    usecase = ShortsCompareUseCase(DummyRepository(my_video, trend_video))
    result = usecase.compare_shorts("youtube", "my123", "trend456")

    assert result["my_video"]["id"] == "my123"
    assert result["trend_video"]["id"] == "trend456"
    assert "hook_comparison" in result
    assert "format_comparison" in result
    assert "reaction_comparison" in result
    assert "ai_summary" in result
    assert "trust_signals" in result
