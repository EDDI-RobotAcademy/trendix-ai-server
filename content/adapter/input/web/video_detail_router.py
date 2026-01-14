from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from content.adapter.input.web.response.video_detail_response import VideoDetailResponse
from content.application.usecase.video_detail_usecase import VideoDetailUseCase
from content.infrastructure.repository.video_detail_repository import VideoDetailRepository


video_detail_router = APIRouter(tags=["analysis"])
usecase = VideoDetailUseCase(VideoDetailRepository())


@video_detail_router.get("/videos/{video_id}", response_model=VideoDetailResponse)
async def get_video_detail(
    video_id: str,
    platform: str | None = Query(default=None),
    history_limit: int = Query(default=9, ge=1, le=30),
):
    try:
        result = usecase.get_video_detail(
            video_id=video_id,
            platform=platform,
            history_limit=history_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))
