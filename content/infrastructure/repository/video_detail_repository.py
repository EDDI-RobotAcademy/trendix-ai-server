from sqlalchemy import text

from config.database.session import SessionLocal


class VideoDetailRepository:
    def __init__(self):
        self.db = SessionLocal()

    def fetch_video_detail(self, video_id: str, platform: str | None = None) -> dict | None:
        # 한국어 주석: 상세 화면에 필요한 핵심 메타 + 채널명 + 카테고리만 조회한다.
        row = self.db.execute(
            text(
                """
                SELECT
                    v.video_id,
                    v.title,
                    v.description,
                    v.channel_id,
                    v.platform,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.published_at,
                    v.thumbnail_url,
                    v.duration,
                    v.category_id,
                    v.is_shorts,
                    COALESCE(ch.title, v.channel_id) AS channel_name,
                    vs.category AS category_name,
                    v.tags
                FROM video v
                LEFT JOIN channel ch ON ch.channel_id = v.channel_id
                LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
                WHERE v.video_id = :video_id
                  AND (:platform IS NULL OR v.platform = :platform)
                """
            ),
            {"video_id": video_id, "platform": platform},
        ).mappings().first()

        if not row:
            return None
        return dict(row)

    def fetch_video_history(
        self,
        video_id: str,
        platform: str | None = None,
        limit: int = 9,
    ) -> list[dict]:
        # 한국어 주석: 최근 스냅샷 기준으로 조회/좋아요 추이를 가져온다.
        rows = self.db.execute(
            text(
                """
                SELECT
                    snapshot_date,
                    view_count,
                    like_count
                FROM video_metrics_snapshot
                WHERE video_id = :video_id
                  AND (:platform IS NULL OR platform = :platform)
                ORDER BY snapshot_date DESC
                LIMIT :limit
                """
            ),
            {"video_id": video_id, "platform": platform, "limit": limit},
        ).mappings().all()

        return [dict(row) for row in rows]
