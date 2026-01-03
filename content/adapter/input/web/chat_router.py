import asyncio
import json
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from openai import OpenAI
from pydantic import BaseModel, Field

from config.settings import OpenAISettings
from content.application.usecase.stopword_usecase import StopwordUseCase
from content.application.usecase.trend_chat_usecase import TrendChatUseCase
from content.application.usecase.trend_featured_usecase import TrendFeaturedUseCase
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from content.infrastructure.repository.stopword_repository_impl import StopwordRepositoryImpl
from content.utils.embedding import EmbeddingService, cosine_similarity

MODEL_NAME = "gpt-4o"


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(user|assistant|system)$")
    content: str = Field(min_length=1)


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    model: str | None = None
    conversationId: str | None = None
    popular_limit: int = 5
    rising_limit: int = 5
    velocity_days: int = 1
    platform: str | None = None


chat_router = APIRouter(tags=["chat"])

repository = ContentRepositoryImpl()
featured_usecase = TrendFeaturedUseCase(repository)
stopword_usecase = StopwordUseCase(StopwordRepositoryImpl.getInstance(), lang="ko")
embedding_service = EmbeddingService(OpenAISettings())
trend_chat_usecase: TrendChatUseCase | None = None
_prototype_embeds: dict[str, list[float]] = {}

# 임베딩 기반 의도 프로토타입 설명 (추천 흐름만 사용)
INTENT_PROTOTYPES = {
    "trend": "사용자가 추천, 트렌드, 인기 영상, 요즘 뜨는 콘텐츠를 묻는 질문",
}


def _extract_last_user_message(messages: list[ChatMessage]) -> str:
    for msg in reversed(messages):
        if msg.role == "user":
            return msg.content
    return ""


def _classify_intent(messages: list[ChatMessage]) -> Literal["trend"]:
    """
    요청 메시지를 임베딩으로 분류해 트렌드 추천 흐름으로만 라우팅한다.
    임베딩이 불가한 경우에도 기본적으로 트렌드 흐름을 사용한다.
    """
    text = _extract_last_user_message(messages)
    cleaned = stopword_usecase.preprocess(text)

    intent_by_embed = _classify_intent_by_embedding(cleaned)
    if intent_by_embed:
        return intent_by_embed
    return "trend"


def _classify_intent_by_embedding(text: str) -> Literal["trend"] | None:
    """
    임베딩을 활용한 트렌드 의도 분류.
    - 임베딩 클라이언트가 없거나 입력이 비어 있으면 None 반환
    - 가장 유사한 프로토타입을 선택하며, 점수 차이가 근소하면 None으로 폴백
    """
    if not text or not embedding_service or not getattr(embedding_service, "client", None):
        return None

    # 프로토타입 임베딩을 한 번만 계산
    if not _prototype_embeds:
        embeds = embedding_service.embed(list(INTENT_PROTOTYPES.values()))
        if not embeds:
            return None
        for label, emb in zip(INTENT_PROTOTYPES.keys(), embeds):
            _prototype_embeds[label] = emb

    query_embed = embedding_service.embed([text])
    if not query_embed:
        return None
    q_vec = query_embed[0]

    scored: list[tuple[str, float]] = []
    for label, proto_vec in _prototype_embeds.items():
        scored.append((label, cosine_similarity(q_vec, proto_vec)))

    scored.sort(key=lambda x: x[1], reverse=True)
    if not scored:
        return None

    best_label, best_score = scored[0]
    if len(scored) > 1 and (best_score - scored[1][1]) < 0.05:
        # 분류 확신이 낮으면 폴백
        return None
    return best_label


def _get_trend_chat_usecase(settings: OpenAISettings) -> TrendChatUseCase:
    global trend_chat_usecase
    if trend_chat_usecase is None:
        trend_chat_usecase = TrendChatUseCase(featured_usecase, settings=settings)
    return trend_chat_usecase


@chat_router.post("/chat/stream")
async def chat_stream(request_body: ChatRequest, request: Request):
    """
    text/event-stream(SSE) 형태로 토큰을 순차 전송합니다.
    질문 의도를 판별해 트렌드 추천/일반 가이드 흐름으로 분기합니다.
    """
    settings = OpenAISettings()
    if not settings.api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not configured")

    intent = _classify_intent(request_body.messages)
    user_messages = [{"role": m.role, "content": m.content} for m in request_body.messages]

    # 일반 챗 모델 이름 기본값
    model = request_body.model or settings.model or MODEL_NAME

    async def event_generator():
        try:
            if request_body.conversationId:
                yield f"data: {json.dumps({'conversationId': request_body.conversationId})}\n\n"

            stream = None

            if intent == "trend":
                usecase = _get_trend_chat_usecase(settings)
                stream, relevant = usecase.answer_with_trends(
                    user_messages=user_messages,
                    popular_limit=request_body.popular_limit,
                    rising_limit=request_body.rising_limit,
                    velocity_days=request_body.velocity_days,
                    platform=request_body.platform,
                )
                yield f"data: {json.dumps({'videos' : relevant}, ensure_ascii=False)}\n\n"
                # data = f"data: {json.dumps({'content': reply, 'relevant': relevant}, ensure_ascii=False)}\n\n"
                # yield data
                # yield "data: [DONE]\n\n"
                # return
            else:
                client = OpenAI(api_key=settings.api_key)
                stream = client.chat.completions.create(
                    model=model,
                    messages=user_messages,
                    stream=True,
                )

            for chunk in stream:
                if await request.is_disconnected():
                    break

                delta = (chunk.choices[0].delta.content or "") if chunk.choices else ""
                if delta:
                    data = f"data: {json.dumps({'content': delta}, ensure_ascii=False)}\n\n"
                    yield data

                await asyncio.sleep(0)

            yield "data: [DONE]\n\n"

        except Exception as exc:
            print(f"Stream error: {exc}")
            yield f"event: error\ndata: {str(exc)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )
