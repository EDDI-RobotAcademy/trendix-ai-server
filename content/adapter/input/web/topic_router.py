from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from content.application.usecase.topic_query_usecase import TopicQueryUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl

topic_router = APIRouter(tags=["topics"])

# 조회 전용 유스케이스/리포지토리 인스턴스
repository = ContentRepositoryImpl()
usecase = TopicQueryUseCase(repository)


@topic_router.get("/category/{category}")
async def get_topics_by_category(
    category: str,
    limit_videos: int = Query(default=20, ge=1, le=100),
    limit_keywords: int = Query(default=10, ge=1, le=100),
):
    """
    카테고리 기반으로 상위 콘텐츠와 주요 키워드를 조회한다.
    """
    result = usecase.query_by_category(category, limit_videos=limit_videos, limit_keywords=limit_keywords)
    if not result["videos"]:
        raise HTTPException(status_code=404, detail="일치하는 카테고리가 없거나 데이터가 없습니다.")
    return JSONResponse(result)


@topic_router.get("/keyword/{keyword}")
async def get_topics_by_keyword(
    keyword: str,
    limit_videos: int = Query(default=20, ge=1, le=100),
    limit_keywords: int = Query(default=10, ge=1, le=100),
):
    """
    키워드 기반으로 상위 콘텐츠와 연관 키워드를 조회한다.
    """
    result = usecase.query_by_keyword(keyword, limit_videos=limit_videos, limit_keywords=limit_keywords)
    if not result["videos"]:
        raise HTTPException(status_code=404, detail="일치하는 키워드가 없거나 데이터가 없습니다.")
    return JSONResponse(result)


@topic_router.get("/video/{video_id}")
async def get_video_detail(video_id: str):
    """
    콘텐츠 단건 상세(점수/키워드 포함)를 조회한다.
    """
    result = usecase.get_video_detail(video_id)
    if not result:
        raise HTTPException(status_code=404, detail="해당 영상이 없습니다.")
    return JSONResponse(result)
