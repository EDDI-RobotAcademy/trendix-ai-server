from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
import os

from content.adapter.input.celery.celery_task_adapter import analyze_video_task
from content.infrastructure.config.dependency_injection import Container, create_container

video_router = APIRouter(tags=["Analysis"])

load_dotenv()

class VideoAnalysisRequest(BaseModel):
    video_id: str
    video_url: HttpUrl
    async_mode: bool = True  # True: Celery, False: 동기 처리


class VideoAnalysisResponse(BaseModel):
    video_id: str
    status: str  # 'processing', 'completed', 'failed'
    message: str
    task_id: Optional[str] = None
    analyzed_at: Optional[datetime] = None


class VideoAnalysisDetailResponse(BaseModel):
    video_id: str
    transcript: str
    transcript_segments: list
    visual_objects: list
    analyzed_at: datetime


# 의존성 주입
def get_container() -> Container:
    return create_container()


@video_router.post("/analyze", response_model=VideoAnalysisResponse)
async def analyze_video(
        request: VideoAnalysisRequest,
        background_tasks: BackgroundTasks,
        container: Container = Depends(get_container)
):
    """
    영상 분석 요청
    - async_mode=True: Celery로 백그라운드 처리
    - async_mode=False: 즉시 동기 처리
    """
    try:
        if request.async_mode:
            # Celery 태스크로 비동기 처리
            task = analyze_video_task.delay(request.video_id, str(request.video_url))

            return VideoAnalysisResponse(
                video_id=request.video_id,
                status="processing",
                message="Video analysis started in background",
                task_id=task.id
            )
        else:
            # FastAPI BackgroundTasks로 처리
            service = container.video_analysis_service()

            async def run_analysis():
                await service.analyze_video(request.video_id, str(request.video_url))

            background_tasks.add_task(run_analysis)

            return VideoAnalysisResponse(
                video_id=request.video_id,
                status="processing",
                message="Video analysis started"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start analysis: {str(e)}")


@video_router.get("/{video_id}/analysis", response_model=VideoAnalysisDetailResponse)
async def get_video_analysis(
        video_id: str,
        container: Container = Depends(get_container)
):
    """영상 분석 결과 조회"""
    try:
        repository = container.video_repository()
        result = await repository.get_analysis(video_id)

        if not result:
            raise HTTPException(status_code=404, detail=f"Analysis not found for video_id: {video_id}")

        return VideoAnalysisDetailResponse(
            video_id=result.video_id,
            transcript=result.transcript_text,
            transcript_segments=[
                {
                    'start': seg.start,
                    'end': seg.end,
                    'text': seg.text
                }
                for seg in result.transcript_segments
            ],
            visual_objects=[
                {
                    'timestamp': frame.timestamp,
                    'objects': [
                        {'class_name': obj.class_name, 'confidence': obj.confidence}
                        for obj in frame.objects
                    ]
                }
                for frame in result.visual_frames
            ],
            analyzed_at=result.analyzed_at
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve analysis: {str(e)}")


@video_router.get("/{video_id}/status")
async def get_analysis_status(
        video_id: str,
        container: Container = Depends(get_container)
):
    """영상 분석 상태 확인"""
    try:
        repository = container.video_repository()
        result = await repository.get_analysis(video_id)

        if not result:
            return {
                'video_id': video_id,
                'status': 'not_found',
                'message': 'Analysis not started or not found'
            }

        return {
            'video_id': video_id,
            'status': 'completed',
            'analyzed_at': result.analyzed_at
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@video_router.post("/{video_id}/embeddings/search")
async def search_similar_content(
        video_id: str,
        query: str,
        limit: int = 10,
        container: Container = Depends(get_container)
):
    """임베딩 기반 유사 콘텐츠 검색"""
    try:
        # 쿼리를 임베딩으로 변환
        embedding_generator = container.embedding_generator()
        query_embedding = await embedding_generator.generate_embedding(query)

        # 유사도 검색
        embedding_repository = container.embedding_repository()
        results = await embedding_repository.search_similar(query_embedding, limit)

        # video_id로 필터링 (선택적)
        if video_id != "all":
            results = [r for r in results if r['video_id'] == video_id]

        return {
            'query': query,
            'results': results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))