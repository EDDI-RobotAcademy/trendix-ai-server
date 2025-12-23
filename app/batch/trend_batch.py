import asyncio
import os
from datetime import date

from sqlalchemy import text

from content.application.usecase.trend_aggregation_usecase import TrendAggregationUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from config.database.session import SessionLocal


async def run_trend_batch_once(as_of: date | None = None, window_days: int = 7, platform: str | None = None) -> dict:
    """
    """
    snapshot_video_metrics(as_of=as_of or date.today(), platform=platform)
    usecase = TrendAggregationUseCase(ContentRepositoryImpl())
    return usecase.aggregate(as_of=as_of, window_days=window_days, platform=platform)


def snapshot_video_metrics(as_of: date, platform: str | None = None) -> None:
    """
    영상 메트릭(조회/좋아요/댓글)을 일별 스냅샷 테이블에 적재해 속도 계산의 기준점을 만듭니다.
    하루 1회 호출을 가정합니다.
    """
    with SessionLocal() as db:
        db.execute(
            text(
                """
                INSERT INTO video_metrics_snapshot (video_id, platform, snapshot_date, view_count, like_count, comment_count)
                SELECT
                    v.video_id,
                    v.platform,
                    :snapshot_date,
                    v.view_count,
                    v.like_count,
                    v.comment_count
                FROM video v
                WHERE (:platform IS NULL OR v.platform = :platform)
                ON CONFLICT (video_id, snapshot_date, platform)
                DO UPDATE SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count
                """
            ),
            {"snapshot_date": as_of, "platform": platform},
        )
        db.commit()


async def start_trend_scheduler():
    """
    - BATCH_TREND_INTERVAL_MINUTES (기본 60), BATCH_TREND_WINDOW_DAYS (기본 7) 사용.
    - BATCH_TREND_LOOKBACK_DAYS: 오늘을 anchor로 N일치 as_of를 역순 실행(예: 3이면 오늘, 어제, 그제)
    """
    if os.getenv("ENABLE_TREND_BATCH", "false").lower() != "true":
        return

    interval_minutes = int(os.getenv("BATCH_TREND_INTERVAL_MINUTES", "60"))
    window_days = int(os.getenv("BATCH_TREND_WINDOW_DAYS", "7"))
    try:
        while True:
            try:
                print("[TREND-BATCH] run started")
            except Exception as exc:
                print("[TREND-BATCH] failed:", exc)
            await asyncio.sleep(interval_minutes * 60)
    except asyncio.CancelledError:
        print("[TREND-BATCH] scheduler stopped")
        raise


if __name__ == "__main__":
    asyncio.run(run_trend_batch_once())
