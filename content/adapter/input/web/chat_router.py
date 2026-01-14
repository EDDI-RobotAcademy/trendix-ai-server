import asyncio
import json
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from openai import OpenAI
from pydantic import BaseModel, Field

from config.settings import OpenAISettings
from content.infrastructure.config.dependency_injection import Container
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
    videos: list[dict] | None = None


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    model: str | None = None
    conversationId: str | None = None
    popular_limit: int = 5
    rising_limit: int = 5
    velocity_days: int = 1
    platform: str | None = None
    videoId: str | None = Field(default=None, alias="video_id")

    class Config:
        populate_by_name = True


chat_router = APIRouter(tags=["chat"])

repository = ContentRepositoryImpl()
featured_usecase = TrendFeaturedUseCase(repository)
stopword_usecase = StopwordUseCase(StopwordRepositoryImpl(), lang="ko")
embedding_service = EmbeddingService(OpenAISettings())
trend_chat_usecase: TrendChatUseCase | None = None
_prototype_embeds: dict[str, list[float]] = {}

# 임베딩 기반 의도 프로토타입 설명
INTENT_PROTOTYPES = {
    "trend": "추천, 요즘 뜨는 트렌드, 인기 있는 유튜버나 영상, 시장 상황을 묻는 질문 (예: 요즘 어떤 영상이 인기야?, 먹방이 대세야?, 재밌는 영상 추천해줘, 볼만한 숏츠 있어?)",
    "guide": "특정 분야나 영상의 제작 방법, 촬영 기술, 편집 노하우, 구성 방식 등 구체적인 가이드를 요청하는 질문 (예: 먹방 어떻게 찍어?, 인트로는 몇 분이 좋아?, 촬영 각도는 어떻게 해?, 저 영상은 어떻게 만들었어?)",
}


def _extract_last_user_message(messages: list[ChatMessage]) -> str:
    for msg in reversed(messages):
        if msg.role == "user":
            return msg.content
    return ""


def _classify_intent(messages: list[ChatMessage]) -> Literal["trend", "guide", "general"]:
    """
    요청 메시지를 임베딩으로 분류해 트렌드 추천, 가이드, 또는 일반 대화로 라우팅한다.
    """
    text = _extract_last_user_message(messages)
    cleaned = stopword_usecase.preprocess(text)

    intent_by_embed = _classify_intent_by_embedding(cleaned)
    if intent_by_embed:
        return intent_by_embed
    return "general"


def _classify_intent_by_embedding(text: str) -> Literal["trend", "guide"] | None:
    """
    임베딩을 활용한 의도 분류.
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
    
    # "제작", "방법", "어떻게", "촬영", "편집" 등의 키워드가 있으면 guide 점수에 가산점 부여 (단순 임베딩 한계 보완)
    keywords = ["제작", "방법", "어떻게", "촬영", "편집", "각도", "분량", "인트로"]
    if any(kw in text for kw in keywords):
        # guide가 1순위가 아니더라도 점수를 높여줌
        for i, (label, score) in enumerate(scored):
            if label == "guide":
                scored[i] = (label, score + 0.1)
        scored.sort(key=lambda x: x[1], reverse=True)
        best_label, best_score = scored[0]

    # "추천", "영상", "콘텐츠", "재밌는거" 등이 있으면 trend 점수에 가산점 부여
    trend_keywords = ["추천", "영상", "콘텐츠", "재밌는", "인기", "볼만한", "뭐 볼까", "쇼츠", "유튜버"]
    if any(kw in text for kw in trend_keywords):
        for i, (label, score) in enumerate(scored):
            if label == "trend":
                scored[i] = (label, score + 0.15)
        scored.sort(key=lambda x: x[1], reverse=True)
        best_label, best_score = scored[0]

    # 임계값 및 차이 검증
    if best_score < 0.25:
        return None
        
    if len(scored) > 1 and (best_score - scored[1][1]) < 0.03:
        # 분류 확신이 아주 낮으면 폴백
        return None
    return best_label


def _get_trend_chat_usecase(settings: OpenAISettings) -> TrendChatUseCase:
    global trend_chat_usecase
    if trend_chat_usecase is None:
        trend_chat_usecase = TrendChatUseCase(featured_usecase, settings=settings)
    return trend_chat_usecase


def _create_error_stream(detail: str):
    """에러 메시지를 SSE 스트림 형식으로 생성"""
    async def generator():
        data = f"data: {json.dumps({'content': detail}, ensure_ascii=False)}\n\n"
        yield data
    return generator()
  
def get_container() -> Container:
    from content.infrastructure.config.dependency_injection import create_container
    return create_container()


@chat_router.post("/chat/stream")
async def chat_stream(
    request_body: ChatRequest, 
    request: Request,
    container: Container = Depends(get_container)
):
    """
    text/event-stream(SSE) 형태로 토큰을 순차 전송합니다.
    질문 의도를 판별해 트렌드 추천/일반 가이드 흐름으로 분기합니다.
    """
    settings = OpenAISettings()
    if not settings.api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not configured")

     # messages 유효성 검사 추가
    if not request_body.messages or len(request_body.messages) == 0:
        raise HTTPException(status_code=400, detail="messages 배열이 비어있습니다.")
    #print(f"request_body.messages:",request_body.messages)
    
    # content가 null/None 또는 공백/* 인 메시지만 필터링하여 제거
    valid_messages = []
    for msg in request_body.messages:
        content = msg.content or ""
        stripped_content = content.strip()
        
        # content가 null이거나 공백/* 이면 스킵
        if stripped_content and stripped_content != '**':
            valid_messages.append(msg)
    
    if len(valid_messages) == 0:
        return StreamingResponse(
            _create_error_stream("유효한 메시지가 없습니다. content가 공백이나 '*' 인 메시지는 제거됩니다."),
            media_type="text/event-stream"
        )
    
    intent = _classify_intent(request_body.messages)
    print("intent  " + intent)
    user_messages = []
    for m in request_body.messages:
        msg = {"role": m.role, "content": m.content}
        if m.videos:
            msg["videos"] = m.videos
        user_messages.append(msg)

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
            elif intent == "guide":
                usecase = container.guide_chat_usecase()
                stream = await usecase.answer_with_guide(
                    user_messages=user_messages,
                    video_id=request_body.videoId
                )
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

