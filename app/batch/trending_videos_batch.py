import asyncio
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from content.application.usecase.ingestion_usecase import IngestionUseCase
from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from config.settings import YouTubeSettings


async def run_trending_videos_batch_once() -> Dict[str, Any]:
    """
    급등/추천 영상을 자동 수집하는 배치의 단일 실행 진입점.
    
    동작:
    - YouTube 인기 급상승 영상 조회
    - 카테고리별 최신 인기 영상 조회
    - Shorts 영상과 일반 영상 구분하여 수집
    - 수집된 영상의 메타데이터와 메트릭 저장
    """
    repository = ContentRepositoryImpl()
    client = YouTubeClient(YouTubeSettings())
    usecase = IngestionUseCase(repository, client)
    
    summary: Dict[str, Any] = {
        "trending_videos": 0,
        "category_videos": 0,
        "shorts_videos": 0,
        "regular_videos": 0,
        "total_videos": 0,
        "categories_processed": [],
        "start_time": datetime.now().isoformat(),
    }
    
    try:
        # 1. YouTube 인기 급상승 영상 수집 (최대 50개)
        print("[TRENDING-BATCH] Collecting trending videos...")
        trending_videos = await _collect_trending_videos(repository, client)
        summary["trending_videos"] = len(trending_videos)
        
        # 2. 주요 카테고리별 인기 영상 수집
        categories = [
            "10",  # Music
            "22",  # People & Blogs  
            "23",  # Comedy
            "24",  # Entertainment
            "25",  # News & Politics
            "26",  # Howto & Style
            "27",  # Education
            "28",  # Science & Technology
        ]
        
        print("[TRENDING-BATCH] Collecting category videos...")
        for category_id in categories:
            try:
                category_videos = await _collect_category_videos(repository, client, category_id)
                summary["category_videos"] += len(category_videos)
                summary["categories_processed"].append({
                    "category_id": category_id,
                    "video_count": len(category_videos)
                })
                print(f"[TRENDING-BATCH] Category {category_id}: {len(category_videos)} videos")
                
                # API 호출 제한을 위한 딜레이
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[TRENDING-BATCH] Error in category {category_id}: {e}")
                continue
        
        # 3. Shorts vs 일반 영상 통계 집계
        all_video_ids = []
        all_video_ids.extend(trending_videos)
        
        shorts_count, regular_count = _count_shorts_vs_regular(repository, all_video_ids)
        summary["shorts_videos"] = shorts_count
        summary["regular_videos"] = regular_count
        summary["total_videos"] = summary["trending_videos"] + summary["category_videos"]
        
        summary["end_time"] = datetime.now().isoformat()
        print(f"[TRENDING-BATCH] Completed successfully: {summary}")
        
    except Exception as e:
        summary["error"] = str(e)
        summary["end_time"] = datetime.now().isoformat()
        print(f"[TRENDING-BATCH] Failed with error: {e}")
        raise
    
    return summary


async def _collect_trending_videos(repository: ContentRepositoryImpl, client: YouTubeClient) -> List[str]:
    """
    YouTube 인기 급상승 영상을 수집합니다.
    """
    try:
        videos = list(client.fetch_trending_videos(max_results=50))
        video_ids = []
        
        for video in videos:
            video.platform = client.platform
            # Shorts 여부 판단 (60초 이하는 Shorts로 간주)
            video = _classify_shorts(video)
            repository.upsert_video(video)
            video_ids.append(video.video_id)
            
        return video_ids
    except Exception as e:
        print(f"[TRENDING-BATCH] Error collecting trending videos: {e}")
        return []


async def _collect_category_videos(repository: ContentRepositoryImpl, client: YouTubeClient, category_id: str) -> List[str]:
    """
    특정 카테고리의 인기 영상을 수집합니다.
    """
    try:
        videos = list(client.fetch_popular_videos_by_category(category_id, max_results=25))
        video_ids = []
        
        for video in videos:
            video.platform = client.platform
            video.category_id = int(category_id)
            # Shorts 여부 판단
            video = _classify_shorts(video)
            repository.upsert_video(video)
            video_ids.append(video.video_id)
            
        return video_ids
    except Exception as e:
        print(f"[TRENDING-BATCH] Error collecting category {category_id} videos: {e}")
        return []


def _classify_shorts(video) -> Any:
    """
    영상이 YouTube Shorts인지 판단하여 is_shorts 필드를 설정합니다.
    
    판단 기준:
    - 영상 길이가 60초 이하
    """
    is_shorts = False
    
    if hasattr(video, 'duration') and video.duration:
        try:
            # YouTube API duration format: PT1M30S -> 90초
            duration_str = video.duration
            seconds = _parse_duration_to_seconds(duration_str)
            
            # 60초 이하면 Shorts로 판단
            is_shorts = seconds <= 60 and seconds > 0
            
        except (ValueError, AttributeError):
            is_shorts = False
    
    # Video 객체에 is_shorts 속성 설정
    video.is_shorts = is_shorts
    return video


def _parse_duration_to_seconds(duration: str) -> int:
    """
    YouTube API duration format (PT1M30S)을 초 단위로 변환합니다.
    """
    if not duration or not duration.startswith('PT'):
        return 0
    
    # PT1M30S, PT45S, PT1H2M3S 등의 형태를 파싱
    duration = duration[2:]  # PT 제거
    
    # 정규식으로 시, 분, 초 추출
    hours_match = re.search(r'(\d+)H', duration)
    minutes_match = re.search(r'(\d+)M', duration)
    seconds_match = re.search(r'(\d+)S', duration)
    
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    seconds = int(seconds_match.group(1)) if seconds_match else 0
    
    return hours * 3600 + minutes * 60 + seconds


def _count_shorts_vs_regular(repository: ContentRepositoryImpl, video_ids: List[str]) -> tuple[int, int]:
    """
    수집된 영상 중 Shorts와 일반 영상 개수를 집계합니다.
    """
    if not video_ids:
        return 0, 0
        
    try:
        from sqlalchemy import text
        from config.database.session import SessionLocal
        
        with SessionLocal() as db:
            result = db.execute(
                text("""
                    SELECT 
                        SUM(CASE WHEN is_shorts = true THEN 1 ELSE 0 END) as shorts_count,
                        SUM(CASE WHEN is_shorts = false OR is_shorts IS NULL THEN 1 ELSE 0 END) as regular_count
                    FROM video 
                    WHERE video_id = ANY(:video_ids)
                """),
                {"video_ids": video_ids}
            ).mappings().one()
            
            return int(result["shorts_count"] or 0), int(result["regular_count"] or 0)
    except Exception as e:
        print(f"[TRENDING-BATCH] Error counting shorts: {e}")
        return 0, 0


async def start_trending_videos_scheduler():
    """
    급등/추천 영상 수집 스케줄러.
    
    - ENABLE_TRENDING_BATCH=true 인 경우에만 동작
    - TRENDING_BATCH_INTERVAL_MINUTES (기본 30분) 주기로 실행
    """
    if os.getenv("ENABLE_TRENDING_BATCH", "false").lower() != "true":
        print("[TRENDING-BATCH] Scheduler disabled (ENABLE_TRENDING_BATCH=false)")
        return
    
    interval_minutes = int(os.getenv("TRENDING_BATCH_INTERVAL_MINUTES", "30"))
    print(f"[TRENDING-BATCH] Scheduler started | interval={interval_minutes}m")
    
    try:
        # 시작 시 즉시 한 번 실행
        try:
            print("[TRENDING-BATCH] Initial run started")
            result = await run_trending_videos_batch_once()
            print("[TRENDING-BATCH] Initial run success:", result)
        except Exception as exc:
            print("[TRENDING-BATCH] Initial run failed:", exc)
        
        # 주기적 실행
        while True:
            await asyncio.sleep(interval_minutes * 60)
            try:
                print("[TRENDING-BATCH] Periodic run started")
                result = await run_trending_videos_batch_once()
                print("[TRENDING-BATCH] Periodic run success:", result)
            except Exception as exc:
                print("[TRENDING-BATCH] Periodic run failed:", exc)
                
    except asyncio.CancelledError:
        print("[TRENDING-BATCH] Scheduler stopped")
        raise


if __name__ == "__main__":
    # 수동 실행: python -m app.batch.trending_videos_batch
    asyncio.run(run_trending_videos_batch_once())