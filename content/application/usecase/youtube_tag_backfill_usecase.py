from typing import Dict

from sqlalchemy import or_

from config.database.session import SessionLocal
from content.application.port.content_repository_port import ContentRepositoryPort
from content.application.port.platform_client_port import PlatformClientPort
from content.domain.keyword_mapping import KeywordMapping
from content.domain.video import Video
from content.infrastructure.orm.models import VideoORM


class YouTubeTagBackfillUseCase:
    """
    이미 DB에 존재하는 영상 중 tags 필드가 비어 있는 레코드만 대상으로,
    YouTube API를 다시 호출해 태그를 채우고 keyword_mapping 까지 생성하는 유스케이스.
    """

    def __init__(self, repository: ContentRepositoryPort, client: PlatformClientPort, session_factory=SessionLocal):
        self.repository = repository
        self.client = client
        self.session_factory = session_factory

    def backfill_missing_tags(self, platform: str = "youtube", limit: int = 50) -> Dict[str, int]:
        """
        - platform: 대상 플랫폼 (기본 youtube)
        - limit: 한 번에 처리할 최대 영상 수 (과도한 API 호출 방지를 위해 페이징 전제)
        """
        with self.session_factory() as db:
            targets: list[VideoORM] = (
                db.query(VideoORM)
                .filter(
                    VideoORM.platform == platform,
                    or_(VideoORM.tags.is_(None), VideoORM.tags == ""),
                )
                .order_by(VideoORM.crawled_at.asc())
                .limit(limit)
                .all()
            )

        video_ids = [v.video_id for v in targets]
        if not video_ids:
            return {"target_count": 0, "updated_count": 0}

        # YouTube API로 메타데이터 재조회
        videos_from_api = list(self.client.fetch_videos_for_ids(video_ids))

        updated = 0
        for video in videos_from_api:
            # API 응답에도 태그가 없다면 업데이트할 필요가 없다.
            if not video.tags:
                continue
            video.platform = platform
            self._persist_video_with_keywords(video)
            updated += 1

        return {"target_count": len(video_ids), "updated_count": updated}

    def _persist_video_with_keywords(self, video: Video) -> None:
        """
        IngestionUseCase._persist_video 와 동일한 규칙으로
        video 및 keyword_mapping 을 upsert 한다.
        """
        self.repository.upsert_video(video)
        if not video.tags:
            return

        keywords = [tag.strip() for tag in video.tags.split(",") if tag.strip()]
        for kw in keywords:
            self.repository.upsert_keyword_mapping(
                KeywordMapping(
                    mapping_id=None,
                    video_id=video.video_id,
                    channel_id=video.channel_id,
                    platform=video.platform,
                    keyword=kw,
                    weight=1.0,
                )
            )


