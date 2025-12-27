from content.application.port.content_repository_port import ContentRepositoryPort


class TrendQueryUseCase:
    def __init__(self, repository: ContentRepositoryPort):
        # 트렌드 탭에서 필요한 조회(핫 트렌드, 추천 콘텐츠)를 담당한다.
        self.repository = repository

    def get_hot_categories(self, platform: str | None = None, limit: int = 20) -> list[dict]:
        return self.repository.fetch_hot_category_trends(platform=platform, limit=limit)

    def get_recommended_contents(
        self, category_id: int, limit: int = 20, days: int = 14, platform: str | None = None
    ) -> list[dict]:
        return self.repository.fetch_recommended_videos_by_category(
            category_id=category_id, limit=limit, days=days, platform=platform
        )

    def get_categories(self, limit: int = 100) -> list[str]:
        return self.repository.fetch_distinct_categories(limit=limit)

    def get_surge_videos(
        self,
        platform: str | None = None,
        limit: int = 30,
        days: int = 3,
        velocity_days: int = 1,
    ) -> list[dict]:
        """
        급등(스파이크) 영상 랭킹을 조회한다.

        - platform: youtube 등 플랫폼 필터
        - limit: 상위 N개
        - days: 최근 N일 내 업로드/수집된 영상만 대상
        - velocity_days: 이전 스냅샷 기준 일수 (예: 1일 전과 비교)
        """
        return self.repository.fetch_surge_videos(
            platform=platform, limit=limit, days=days, velocity_days=velocity_days
        )