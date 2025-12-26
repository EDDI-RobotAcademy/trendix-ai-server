import os
import boto3
import asyncio
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from account.adapter.input.web.account_router import account_router
from content.adapter.input.web.chat_router import chat_router
from content.adapter.input.web.ingestion_router import ingestion_router
from content.adapter.input.web.topic_router import topic_router
from content.adapter.input.web.trend_router import trend_router
from social_oauth.adapter.input.web.google_oauth2_router import authentication_router
from app.scheduler.youtube_trend_scheduler import start_youtube_trend_scheduler, stop_youtube_trend_scheduler
from app.scheduler.api.scheduler_router import router as scheduler_router
from config.database.session import init_db_schema
from social_oauth.adapter.input.web.logout_router import logout_router

from content.infrastructure.middleware.stopword_middleware import StopwordMiddleware


load_dotenv()

os.environ.setdefault("CUDA_LAUNCH_BLOCKING", "1")
os.environ.setdefault("TORCH_USE_CUDA_DSA", "1")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan 훅을 활용해 배치 태스크와 리소스를 관리합니다.
    """
    # DB 스키마 미존재 시 자동 생성하여 UndefinedTable 오류를 예방합니다.
    init_db_schema()
    
    # YouTube 트렌드 스케줄러 시작 (기존 배치 시스템 대체)
    app.state.youtube_trend_task = asyncio.create_task(start_youtube_trend_scheduler())
    
    try:
        yield
    finally:
        # YouTube 트렌드 스케줄러 정리
        youtube_trend_task = getattr(app.state, "youtube_trend_task", None)
        if youtube_trend_task:
            try:
                await stop_youtube_trend_scheduler()
            except Exception:
                pass
            youtube_trend_task.cancel()


app = FastAPI(title="Apple Mango AI Server", version="0.1.0", lifespan=lifespan)

origins_env = os.getenv("CORS_ORIGINS")
origins = [origin for origin in origins_env.split(",") if origin] if origins_env else ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 불용어 처리 미들웨어
app.add_middleware(
    StopwordMiddleware
)

app.include_router(account_router, prefix="/accounts")
app.include_router(chat_router)
app.include_router(authentication_router, prefix="/authentication")
app.include_router(ingestion_router, prefix="/ingestion")
app.include_router(topic_router, prefix="/topics")
app.include_router(trend_router, prefix="/trends")
app.include_router(scheduler_router, prefix="/api/v1")
app.include_router(logout_router, prefix="/logout")

@app.get("/health")
def health_check() -> dict[str, str]:
    """
    헬스체크 엔드포인트입니다.
    """
    return {"status": "ok"}


@app.post("/test")
async def test_endpoint(request: Request):
    """
    불용어 처리 테스트 엔드포인트입니다.
    """
    data = await request.json()
    print(f"data={data}")
    return {"processed_text": data.get("text")}


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
