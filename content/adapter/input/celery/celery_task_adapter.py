from celery import Celery
import asyncio
import os
from dotenv import load_dotenv
import torch

from content.infrastructure.config.dependency_injection import Container, create_container

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

app = Celery('video_analyzer', broker=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}',)

# Celery 설정
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1시간
    task_soft_time_limit=3300,  # 55분
)


def get_container() -> Container:
    """의존성 주입 컨테이너 생성"""
    return create_container()

async def _run_analyze_video(service, video_id: str, video_url: str):
    """
    비동기 분석 서비스를 실행하고 사용된 DB 엔진을 명시적으로 정리합니다.
    """
    try:
        return await service.analyze_video(video_id, video_url)
    finally:
        # DB 연결 풀 정리 (이벤트 루프가 닫히기 전에 실행되어야 함)
        from config.database.session import async_engine
        await async_engine.dispose()

@app.task(bind=True, max_retries=3)
def analyze_video_task(self, video_id: str, video_url: str):
    """
    Celery 태스크 어댑터
    - 영상 분석을 백그라운드에서 처리
    - 실패 시 3번까지 재시도
    """
    try:
        container = get_container()
        service = container.video_analysis_service()

        # asyncio.run()은 새 루프를 생성하고 종료 시 안전하게 정리합니다.
        result = asyncio.run(_run_analyze_video(service, video_id, video_url))

        return {
            'video_id': result.video_id,
            'status': 'completed',
            'analyzed_at': result.analyzed_at.isoformat()
        }

    except Exception as exc:
        # 재시도
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@app.task
def cleanup_old_videos():
    """오래된 임시 파일 정리 (주기적 실행)"""
    import os
    from pathlib import Path
    from datetime import datetime, timedelta

    temp_dir = Path("/tmp/videos")
    cutoff_time = datetime.now() - timedelta(hours=24)

    for file_path in temp_dir.glob("video_*.mp4"):
        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if file_time < cutoff_time:
            try:
                file_path.unlink()
                print(f"Deleted old file: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")


# Celery Beat 스케줄 (주기적 작업)
app.conf.beat_schedule = {
    'cleanup-every-hour': {
        'task': 'infrastructure.adapters.input.celery_task_adapter.cleanup_old_videos',
        'schedule': 3600.0,  # 1시간마다
    },
}