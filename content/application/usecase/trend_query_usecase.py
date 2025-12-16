from content.application.port.content_repository_port import ContentRepositoryPort


class TrendQueryUseCase:
    def __init__(self, repository: ContentRepositoryPort):
        # 트렌드 탭에서 필요한 조회(핫 트렌드, 추천 콘텐츠)를 담당한다.
        self.repository = repository

    def get_hot_categories(self, platform: str | None = None, limit: int = 20) -> list[dict]:
        return self.repository.fetch_hot_category_trends(platform=platform, limit=limit)

    def get_recommended_contents(
        self, category: str, limit: int = 20, days: int = 14, platform: str | None = None
    ) -> list[dict]:
        return self.repository.fetch_recommended_videos_by_category(
            category=category, limit=limit, days=days, platform=platform
        )

    def get_categories(self, limit: int = 100) -> list[str]:
        return self.repository.fetch_distinct_categories(limit=limit)
