from datetime import date, datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text

from config.settings import OpenAISettings, YouTubeSettings
from config.database.session import SessionLocal
from content.adapter.input.web.request.ingest_requests import IngestChannelRequest, IngestVideoRequest
from content.application.usecase.ingestion_usecase import IngestionUseCase
from content.application.usecase.sentiment_usecase import SentimentUseCase
from content.application.usecase.trend_aggregation_usecase import TrendAggregationUseCase
from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl

ingestion_router = APIRouter(tags=["ingestion"])

# 공용 리포지토리/유스케이스를 미리 생성해 재사용한다.
repository = ContentRepositoryImpl()
sentiment_usecase = SentimentUseCase(OpenAISettings()) if OpenAISettings().api_key else None
ingestion_usecase = IngestionUseCase(repository, sentiment_usecase)


def resolve_platform_client(platform: str):
    """
    현재는 youtube만 지원. 다른 플랫폼은 향후 확장 예정.
    """
    platform = platform.lower()
    if platform == "youtube":
        return YouTubeClient(YouTubeSettings())
    raise HTTPException(status_code=400, detail="지원하지 않는 플랫폼입니다. (현재 youtube만 사용 가능)")


@ingestion_router.get("/{platform}/video/{video_id}/analysis")
async def get_video_analysis(platform: str, video_id: str):
    """
    AI 분석 결과 조회용 엔드포인트.
    - 영상 메타 + 감정/카테고리/트렌드 + 점수 + 키워드 매핑 + 댓글 감정 리스트를 반환한다.
    """
    db = SessionLocal()
    try:
        video = db.execute(
            """
            SELECT v.video_id, v.title, v.channel_id, v.platform, v.view_count, v.like_count, v.comment_count,
                   vs.category, vs.sentiment_label, vs.sentiment_score, vs.trend_score, vs.keywords, vs.summary,
                   sc.engagement_score, sc.sentiment_score AS score_sentiment, sc.trend_score AS score_trend, sc.total_score,
                   vs.analyzed_at
            FROM video v
            LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
            LEFT JOIN video_score sc ON sc.video_id = v.video_id
            WHERE v.video_id = :video_id AND v.platform = :platform
            """,
            {"video_id": video_id, "platform": platform},
        ).mappings().first()

        if not video:
            raise HTTPException(status_code=404, detail="영상이 존재하지 않습니다.")

        keywords = db.execute(
            """
            SELECT keyword, weight, platform, video_id, channel_id
            FROM keyword_mapping
            WHERE video_id = :video_id
            ORDER BY weight DESC NULLS LAST, keyword
            """,
            {"video_id": video_id},
        ).mappings().all()

        comment_sentiments = db.execute(
            """
            SELECT comment_id, video_id, platform, sentiment_label, sentiment_score, analyzed_at
            FROM comment_sentiment
            WHERE video_id = :video_id
            ORDER BY analyzed_at DESC
            """,
            {"video_id": video_id},
        ).mappings().all()

        return {
            "video": dict(video),
            "keywords": [dict(k) for k in keywords],
            "comment_sentiments": [dict(c) for c in comment_sentiments],
        }
    finally:
        db.close()


@ingestion_router.post("/{platform}/channel/{channel_id}")
async def ingest_channel(platform: str, channel_id: str, request: IngestChannelRequest):
    """
    채널 단위로 영상/댓글을 수집하고 AI 분석까지 수행한다.
    """
    client = resolve_platform_client(platform)
    try:
        result = ingestion_usecase.ingest_channel_bundle(
            client,
            channel_id,
            include_comments=request.include_comments,
            max_videos=request.max_videos,
            max_comments=request.max_comments,
        )
        return JSONResponse(result)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@ingestion_router.post("/{platform}/video/{video_id}")
async def ingest_video(platform: str, video_id: str, request: IngestVideoRequest):
    """
    단일 영상에 대해 본문/댓글 수집 및 AI 분석을 수행한다.
    """
    client = resolve_platform_client(platform)
    try:
        result = ingestion_usecase.ingest_video(
            client,
            video_id,
            include_comments=request.include_comments,
            max_comments=request.max_comments,
        )
        return JSONResponse(result)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@ingestion_router.post("/trend/aggregate")
async def trigger_trend_batch(window_days: int = 7, platform: str | None = None):
    """
    수동 배치 실행용 엔드포인트.
    - 최근 window_days 동안 수집된 콘텐츠를 바탕으로 카테고리/키워드 트렌드 테이블을 갱신한다.
    """
    usecase = TrendAggregationUseCase(ContentRepositoryImpl())
    result = usecase.aggregate(window_days=window_days, platform=platform)
    return result


@ingestion_router.get("/category_Tags")
async def get_category_tags():
    """
    category_trend_tag 테이블의 태그 정보를 조회하는 엔드포인트.

    - DB에 저장된 tags 문자열 그대로(`tags_raw`)와
    - 콤마로 분리한 해시태그 리스트(`tags`)를 함께 반환한다.
    """
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT category, tags, create_at
                FROM category_trend_tag
                ORDER BY category
                """
            )
        ).mappings().all()

        items = []
        for row in rows:
            tags_str = row["tags"] or ""
            tags_list = [t.strip() for t in tags_str.split(",") if t.strip()]

            created_raw = row["create_at"]
            if isinstance(created_raw, (datetime, date)):
                created_value = created_raw.isoformat()
            else:
                # 이미 문자열이거나 다른 타입인 경우 그대로 반환
                created_value = created_raw

            items.append(
                {
                    "category": row["category"],
                    "tags_raw": tags_str,  # DB에 저장된 원본 문자열
                    "tags": tags_list,     # 리스트 형태의 해시태그
                    "create_at": created_value,
                }
            )

        return {"items": items}
    finally:
        db.close()

# Postman 참고:
# 1) 건강 확인: GET http://localhost:8000/health
# 2) 채널 수집: POST http://localhost:8000/ingestion/youtube/channel/<CHANNEL_ID>
# 3) 영상 수집: POST http://localhost:8000/ingestion/youtube/video/<VIDEO_ID>
# 4) 분석 조회: GET  http://localhost:8000/ingestion/youtube/video/<VIDEO_ID>/analysis
# 5) 트렌드 집계: POST http://localhost:8000/ingestion/trend/aggregate
# 6) 카테고리 집계 : GET http://localhost:8000/ingestion/category_Tags
