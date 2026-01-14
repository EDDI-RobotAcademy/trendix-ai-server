from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from content.adapter.input.web.request.channel_analysis_request import ChannelAnalysisRequest
from content.adapter.input.web.response.channel_analysis_response import ChannelAnalysisResponse
from content.application.usecase.channel_analysis_usecase import ChannelAnalysisUseCase
from content.infrastructure.repository.channel_analysis_repository import ChannelAnalysisRepository


channel_analysis_router = APIRouter(tags=["analysis"])
usecase = ChannelAnalysisUseCase(ChannelAnalysisRepository())


@channel_analysis_router.post("/channel", response_model=ChannelAnalysisResponse)
async def analyze_channel(request: ChannelAnalysisRequest):
    if request.platform.lower() != "youtube":
        raise HTTPException(status_code=400, detail="지원하지 않는 플랫폼입니다. (현재 youtube만 가능)")

    try:
        result = usecase.analyze_channel(
            platform=request.platform,
            channel_identifier=request.channel_url,
            limit=request.limit,
            trend_days=request.trend_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))
