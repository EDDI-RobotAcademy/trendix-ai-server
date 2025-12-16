import asyncio
import os

from content.application.usecase.trend_aggregation_usecase import TrendAggregationUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl


async def run_trend_batch_once(as_of: date | None = None, window_days: int = 7, platform: str | None = None) -> dict:
    """
    """
    usecase = TrendAggregationUseCase(ContentRepositoryImpl())
    return usecase.aggregate(as_of=as_of, window_days=window_days, platform=platform)


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
