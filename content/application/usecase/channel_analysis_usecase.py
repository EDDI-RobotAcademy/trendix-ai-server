from datetime import datetime

from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.channel_analysis_repository import ChannelAnalysisRepository
from config.settings import YouTubeSettings


class ChannelAnalysisUseCase:
    def __init__(self, repository: ChannelAnalysisRepository):
        self.repository = repository

    def analyze_channel(
        self,
        platform: str,
        channel_identifier: str,
        limit: int = 6,
        trend_days: int = 14,
    ) -> dict:
        client = self._resolve_platform_client(platform)
        channel_id = client._resolve_channel_id(channel_identifier)

        channel_payload = self._fetch_channel_payload(client, channel_id)
        channel_name = channel_payload["title"]
        channel_thumbnail = channel_payload["thumbnail_url"]
        subscriber_count = channel_payload["subscriber_count"]
        total_videos = channel_payload["video_count"]

        videos = list(client.fetch_videos(channel_id, max_results=limit))
        channel_avg_views = self._calculate_channel_average_views(videos)

        recent_videos = []
        ratios = []
        for video in videos:
            trend_avg = self.repository.fetch_category_average_views(
                video.category_id,
                platform=client.platform,
                days=trend_days,
            )
            trend_avg = trend_avg or channel_avg_views or float(video.view_count or 0)
            trend_avg = max(trend_avg, 1.0)

            view_count = int(video.view_count or 0)
            ratio = view_count / trend_avg if trend_avg else 0.0
            ratios.append(ratio)

            recent_videos.append(
                {
                    "id": video.video_id,
                    "title": video.title or "",
                    "thumbnailUrl": video.thumbnail_url,
                    "viewCount": view_count,
                    "trendAvgViewCount": int(trend_avg),
                    "publishedAt": self._format_datetime(video.published_at),
                    "performance": self._classify_performance(ratio),
                }
            )

        overall_performance = self._calculate_overall_performance(ratios)

        return {
            "channelId": channel_id,
            "channelName": channel_name,
            "channelThumbnail": channel_thumbnail,
            "subscriberCount": subscriber_count,
            "totalVideos": total_videos,
            "overallPerformance": overall_performance,
            "recentVideos": recent_videos,
        }

    @staticmethod
    def _resolve_platform_client(platform: str) -> YouTubeClient:
        # 한국어 주석: 현재는 youtube만 지원한다.
        platform = platform.lower()
        if platform != "youtube":
            raise ValueError("지원하지 않는 플랫폼입니다. (현재 youtube만 가능)")
        return YouTubeClient(YouTubeSettings())

    @staticmethod
    def _fetch_channel_payload(client: YouTubeClient, channel_id: str) -> dict:
        # 한국어 주석: 채널 프로필과 통계를 한 번에 얻어 프론트 표시용 값을 만든다.
        response = client.service.channels().list(part="snippet,statistics", id=channel_id).execute()
        items = response.get("items", [])
        if not items:
            raise ValueError("채널 정보를 찾을 수 없습니다.")

        snippet = items[0].get("snippet", {})
        stats = items[0].get("statistics", {})
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (
            (thumbnails.get("high") or {}).get("url")
            or (thumbnails.get("medium") or {}).get("url")
            or (thumbnails.get("default") or {}).get("url")
        )

        return {
            "title": snippet.get("title", ""),
            "thumbnail_url": thumbnail_url,
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
        }

    @staticmethod
    def _calculate_channel_average_views(videos: list) -> float:
        # 한국어 주석: 비교 기준이 없을 때 채널 평균 조회수를 fallback으로 사용한다.
        counts = [int(v.view_count or 0) for v in videos if v.view_count is not None]
        if not counts:
            return 0.0
        return sum(counts) / len(counts)

    @staticmethod
    def _classify_performance(ratio: float) -> str:
        if ratio >= 1.2:
            return "above"
        if ratio >= 0.8:
            return "average"
        return "below"

    @staticmethod
    def _calculate_overall_performance(ratios: list[float]) -> int:
        if not ratios:
            return 0
        average_ratio = sum(ratios) / len(ratios)
        return max(0, min(100, int(round(average_ratio * 100))))

    @staticmethod
    def _format_datetime(value: datetime | None) -> str:
        if value is None:
            return ""
        return value.isoformat()
