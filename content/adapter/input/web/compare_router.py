from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from content.adapter.input.web.request.shorts_compare_request import ShortsCompareRequest
from content.application.usecase.shorts_compare_usecase import ShortsCompareUseCase, ShortsCompareDurationError
from content.application.usecase.ingestion_usecase import IngestionUseCase
from content.application.usecase.sentiment_usecase import SentimentUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from content.infrastructure.client.youtube_client import YouTubeClient
from config.settings import OpenAISettings, YouTubeSettings
from content.utils.youtube_url import parse_youtube_video_id


compare_router = APIRouter(tags=["analysis"])

repository = ContentRepositoryImpl()
usecase = ShortsCompareUseCase(repository)
_sentiment_usecase: SentimentUseCase | None = None


def _get_sentiment_usecase() -> SentimentUseCase | None:
    # 한국어 주석: OpenAI 키가 없으면 감정 분석은 생략한다.
    global _sentiment_usecase
    if _sentiment_usecase is not None:
        return _sentiment_usecase
    settings = OpenAISettings()
    if not settings.api_key:
        return None
    _sentiment_usecase = SentimentUseCase(settings)
    return _sentiment_usecase


def _resolve_platform_client(platform: str):
    # 한국어 주석: 현재는 youtube만 허용하며, 필요 시 확장한다.
    platform = platform.lower()
    if platform == "youtube":
        return YouTubeClient(YouTubeSettings())
    raise HTTPException(status_code=400, detail="지원하지 않는 플랫폼입니다. (현재 youtube만 가능)")


def _ensure_video_ingested(video_id: str, platform: str) -> None:
    # 한국어 주석: DB에 없으면 즉시 수집하여 비교 분석이 가능하도록 한다.
    existing = repository.fetch_video_summary(video_id, platform=platform)
    if existing:
        return

    try:
        client = _resolve_platform_client(platform)
        ingestion_usecase = IngestionUseCase(repository, _get_sentiment_usecase())
        ingestion_usecase.ingest_video(client, video_id, include_comments=False, max_comments=0)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"영상 수집에 실패했습니다: {exc}") from exc

    if not repository.fetch_video_summary(video_id, platform=platform):
        raise HTTPException(status_code=404, detail="영상 정보를 찾을 수 없습니다.")


@compare_router.post("/shorts/compare")
async def compare_shorts(request: ShortsCompareRequest):
    if request.platform != "youtube":
        raise HTTPException(status_code=400, detail="지원하지 않는 플랫폼입니다. (현재 youtube만 사용 가능)")

    my_video_id = parse_youtube_video_id(request.my_short_url)
    trend_video_id = parse_youtube_video_id(request.trend_short_url)

    # 한국어 주석: URL에서 영상 ID를 추출하지 못하면 즉시 오류를 반환한다.
    if not my_video_id:
        raise HTTPException(status_code=400, detail="내 쇼츠 URL에서 video_id를 추출할 수 없습니다.")
    if not trend_video_id:
        raise HTTPException(status_code=400, detail="급등 쇼츠 URL에서 video_id를 추출할 수 없습니다.")

    try:
        _ensure_video_ingested(my_video_id, request.platform)
        _ensure_video_ingested(trend_video_id, request.platform)
        result = usecase.compare_shorts(
            platform=request.platform,
            my_video_id=my_video_id,
            trend_video_id=trend_video_id,
        )
    except ShortsCompareDurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))
