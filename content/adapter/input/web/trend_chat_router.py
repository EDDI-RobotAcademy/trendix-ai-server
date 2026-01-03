from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from config.settings import OpenAISettings
from content.application.usecase.trend_featured_usecase import TrendFeaturedUseCase
from content.application.usecase.trend_chat_usecase import TrendChatUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl


class TrendChatMessage(BaseModel):
    role: str = Field(pattern="^(user|assistant|system)$")
    content: str = Field(min_length=1)


class TrendChatRequest(BaseModel):
    messages: list[TrendChatMessage]
    popular_limit: int = 5
    rising_limit: int = 5
    velocity_days: int = 1
    platform: str | None = None


class TrendChatResponse(BaseModel):
    reply: str


trend_chat_router = APIRouter(tags=["chat-trend"])

repository = ContentRepositoryImpl()
featured_usecase = TrendFeaturedUseCase(repository)
trend_chat_usecase = TrendChatUseCase(featured_usecase)


@trend_chat_router.post("/chat/trends", response_model=TrendChatResponse)
async def chat_with_trends(
    request: TrendChatRequest,
):
    """
    트렌드 데이터를 컨텍스트로 주입해 답변하는 전용 챗 엔드포인트.
    """
    try:
        result = trend_chat_usecase.answer_with_trends(
            user_messages=[m.model_dump() for m in request.messages],
            popular_limit=request.popular_limit,
            rising_limit=request.rising_limit,
            velocity_days=request.velocity_days,
            platform=request.platform,
        )
        reply = result[0] if isinstance(result, (list, tuple)) else result
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:  # OpenAI 예외 일괄 처리
        raise HTTPException(status_code=500, detail=f"Trend chat request failed: {exc}")
    return TrendChatResponse(reply=reply)
