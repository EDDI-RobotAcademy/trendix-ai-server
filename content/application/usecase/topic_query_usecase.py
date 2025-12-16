from content.application.port.content_repository_port import ContentRepositoryPort


class TopicQueryUseCase:
    def __init__(self, repository: ContentRepositoryPort):
        # 카테고리/키워드 기반 조회를 담당하는 유스케이스
        self.repository = repository

    def query_by_category(self, category: str, limit_videos: int = 20, limit_keywords: int = 10) -> dict:
        videos = self.repository.fetch_videos_by_category(category, limit=limit_videos)
        keywords = self.repository.fetch_top_keywords_by_category(category, limit=limit_keywords)
        return {"category": category, "videos": videos, "keywords": keywords}

    def query_by_keyword(self, keyword: str, limit_videos: int = 20, limit_keywords: int = 10) -> dict:
        videos = self.repository.fetch_videos_by_keyword(keyword, limit=limit_videos)
        keywords = self.repository.fetch_top_keywords_by_keyword(keyword, limit=limit_keywords)
        return {"keyword": keyword, "videos": videos, "keywords": keywords}

    def get_video_detail(self, video_id: str) -> dict | None:
        """
        단일 콘텐츠 상세 조회를 제공한다.
        """
        return self.repository.fetch_video_with_scores(video_id)
