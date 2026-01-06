from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from content.application.usecase.trend_query_usecase import TrendQueryUseCase
from content.application.usecase.trend_featured_usecase import TrendFeaturedUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl

trend_router = APIRouter(tags=["trends"])

# 트렌드 탭 전용 조회용 유즈케이스/리포지토리 싱글턴
repository = ContentRepositoryImpl()
usecase = TrendQueryUseCase(repository)
featured_usecase = TrendFeaturedUseCase(repository)


@trend_router.get("/categories/hot")
async def get_hot_categories(
    limit: int = Query(default=20, ge=1, le=100),
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
):
    """
    카테고리별 최신 집계(랭킹 순) 리스트를 조회한다.
    """
    result = usecase.get_hot_categories(platform=platform, limit=limit)
    if not result:
        raise HTTPException(status_code=404, detail="집계된 카테고리 트렌드가 없습니다.")
    # datetime/date 등이 JSON 직렬화 오류를 내지 않도록 변환
    return JSONResponse(jsonable_encoder({"items": result}))


@trend_router.get("/categories/{category}/recommendations")
async def get_category_recommendations(
    category: str,
    limit: int = Query(default=20, ge=1, le=100),
    days: int = Query(default=14, ge=1, le=90, description="최근 N일 내 수집본만 대상으로 추천"),
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
):
    """
    카테고리 문자열(category) 기준 추천 콘텐츠(점수/신선도 기반)를 조회한다.
    - 예: /trends/categories/게임/recommendations?limit=20&days=14&platform=youtube
    """
    items = usecase.get_recommended_contents(
        category=category, limit=limit, days=days, platform=platform
    )
    if not items:
        raise HTTPException(status_code=404, detail="추천 가능한 콘텐츠가 없습니다.")
    # datetime/date 등이 JSON 직렬화 오류를 내지 않도록 변환
    return JSONResponse(jsonable_encoder({"category": category, "items": items}))


@trend_router.get("/categories")
async def list_categories(limit: int = Query(default=100, ge=1, le=500)):
    """
    관심사 등록용 카테고리 목록을 조회한다.
    """
    categories = usecase.get_categories(limit=limit)
    if not categories:
        raise HTTPException(status_code=404, detail="등록된 카테고리가 없습니다.")
    return JSONResponse(jsonable_encoder({"categories": categories}))


@trend_router.get("/menu")
async def get_videos_by_category_id(
    category_id: int = Query(..., description="YouTube 카테고리 ID (숫자)"),
    limit: int = Query(default=20, ge=1, le=100),
    days: int = Query(default=14, ge=1, le=90, description="최근 N일 내 게시된 영상만 대상"),
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
):
    """
    category_id 기준으로 최근 N일 내 영상 리스트를 조회한다.

    - 요청 예시:
      /trends/menu?category_id=20&limit=20&days=14&platform=youtube

    - 응답:
      { "category_id": 20, "items": [ { video_id, title, ... }, ... ] }
    """
    items = usecase.get_videos_by_category_id(
        category_id=category_id,
        limit=limit,
        days=days,
        platform=platform,
    )
    if not items:
        raise HTTPException(status_code=404, detail="해당 카테고리의 영상이 없습니다.")
    return JSONResponse(jsonable_encoder({"category_id": category_id, "items": items}))


@trend_router.get("/videos/surge")
async def get_surge_videos(
    limit: int = Query(default=30, ge=1, le=100),
    days: int = Query(default=3, ge=1, le=30, description="최근 N일 내 업로드/수집된 영상만 대상"),
    velocity_days: int = Query(
        default=1,
        ge=1,
        le=30,
        description="단기 증가량/증가율 비교 기준 일수 (예: N일 전과 비교)",
    ),
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
):
    """
    급등(스파이크) 영상 랭킹 리스트를 조회한다.

    - 요청 예시:
      /trends/videos/surge?platform=youtube&limit=20&days=3&velocity_days=1

    - 응답:
      { "items": [ { video_id, title, channel_id, view_count, ... }, ... ] }
    """
    items = usecase.get_surge_videos(
        platform=platform,
        limit=limit,
        days=days,
        velocity_days=velocity_days,
    )
    if not items:
        raise HTTPException(status_code=404, detail="급등 영상이 없습니다.")
    return JSONResponse(jsonable_encoder({"items": items}))


@trend_router.get("/videos/{video_id}/view_history")
async def get_video_view_history(
    video_id: str,
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
    limit: int | None = Query(
        default=None,
        ge=1,
        le=365,
        description="조회수 히스토리를 조회할 최대 일수 (미지정 시 전체)",
    ),
):
    """
    특정 영상의 일자별(view_count, like_count, comment_count) 히스토리를 조회한다.

    - 기본: 모든 스냅샷을 snapshot_date 최신순으로 반환
    - limit 지정 시 최신 N일치만 반환

    응답 예:
    {
      "video_id": "...",
      "platform": "youtube",
      "history": [
        { "snapshot_date": "2025-01-10", "view_count": 12345, "like_count": 123, "comment_count": 10 },
        { "snapshot_date": "2025-01-09", "view_count": 10000, "like_count": 90, "comment_count": 8 },
        ...
      ]
    }
    """
    items = usecase.get_video_view_history(
        video_id=video_id,
        platform=platform,
        limit=limit,
    )
    if not items:
        raise HTTPException(status_code=404, detail="해당 영상의 조회 이력이 없습니다.")

    # video_id / platform은 모두 동일하므로 첫 번째 값 기준으로 응답 구성
    first = items[0]
    history = [
        {
            "snapshot_date": row["snapshot_date"],
            "view_count": row["view_count"],
            "like_count": row.get("like_count"),
            "comment_count": row.get("comment_count"),
        }
        for row in items
    ]

    return JSONResponse(
        jsonable_encoder(
            {
                "video_id": first["video_id"],
                "platform": first.get("platform"),
                "history": history,
            }
        )
    )


@trend_router.get("/featured")
async def get_featured_trends(
    popular_limit: int = Query(default=5, ge=1, le=20),
    rising_limit: int = Query(default=5, ge=1, le=20),
    velocity_days: int = Query(default=1, ge=1, le=7),
    platform: str | None = Query(default=None, description="플랫폼 필터 (예: youtube)"),
):
    """
    인기(Popular)와 급상승(Rising) 후보를 분리해 반환한다.
    - Popular: 절대 조회/점수 기반 상위 popular_limit
    - Rising: 최근 증가량(velocity_days 기준) 상위 rising_limit
    - categories: 최신 카테고리 트렌드 상위 5
    """
    result = featured_usecase.get_featured(
        limit_popular=popular_limit,
        limit_rising=rising_limit,
        velocity_days=velocity_days,
        platform=platform,
    )
    if not result["popular"] and not result["rising"]:
        raise HTTPException(status_code=404, detail="추천할 데이터가 없습니다.")
    return JSONResponse(jsonable_encoder(result))


@trend_router.get("/videos/{video_id}/history")
async def get_video_snapshot_history(
    video_id: str,
    days: int = Query(default=7, ge=1, le=30, description="최근 N일간의 스냅샷 히스토리"),
    platform: str = Query(default="youtube", description="플랫폼 (기본: youtube)"),
):
    """
    특정 영상의 스냅샷 히스토리를 조회하여 추이 차트 데이터를 제공한다.
    스냅샷이 없는 경우 현재 video 데이터를 반환한다.

    - 요청 예시:
      /trends/videos/abc123/history?days=7&platform=youtube

    - 응답:
      { "items": [ { snapshot_date, view_count, like_count, comment_count, daily_*_increase }, ... ] }
    """
    items = usecase.get_video_snapshot_history(
        video_id=video_id,
        platform=platform,
        days=days,
    )
    # 빈 배열도 허용 (프론트엔드에서 처리)
    return JSONResponse(jsonable_encoder({"video_id": video_id, "items": items}))

