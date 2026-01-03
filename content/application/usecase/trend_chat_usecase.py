from __future__ import annotations

from typing import List, Tuple

from fastapi.encoders import jsonable_encoder
from openai import OpenAI, Stream
from openai.types.chat import ChatCompletionChunk

from config.settings import OpenAISettings
from content.application.usecase.trend_featured_usecase import TrendFeaturedUseCase
from content.utils.embedding import EmbeddingService, cosine_similarity


class TrendChatUseCase:
    """
    트렌드 데이터를 컨텍스트로 주입한 챗 응답을 생성하는 유스케이스.
    - 인기/급상승/카테고리/추천(질의 기반 재정렬)을 LLM에 넣어 답변을 생성한다.
    """

    def __init__(
        self,
        featured_usecase: TrendFeaturedUseCase,
        settings: OpenAISettings | None = None,
        embedding_service: EmbeddingService | None = None,
    ):
        self.featured_usecase = featured_usecase
        self.settings = settings or OpenAISettings()
        if not self.settings.api_key:
            raise ValueError("OPENAI_API_KEY is required for TrendChatUseCase")
        self.client = OpenAI(api_key=self.settings.api_key)
        self.embedding_service = embedding_service or EmbeddingService(self.settings)

    def answer_with_trends(
        self,
        user_messages: List[dict],
        popular_limit: int = 5,
        rising_limit: int = 5,
        velocity_days: int = 1,
        platform: str | None = None,
    ) -> Tuple[Stream[ChatCompletionChunk], Tuple[str, list[dict]]]:
        # 유저 질문 추출 (마지막 user 메시지)
        query = ""
        for msg in reversed(user_messages):
            if msg.get("role") == "user":
                query = msg.get("content", "")
                break

        trends = self.featured_usecase.get_featured(
            limit_popular=popular_limit,
            limit_rising=rising_limit,
            velocity_days=velocity_days,
            platform=platform,
            query=query or None,
        )

        if not trends.get("popular") and not trends.get("rising"):
            return "트렌드 데이터가 부족해요. 나중에 다시 시도해 주세요.", []

        context_text = self._build_context(trends)
        relevant = self._retrieve_relevant_items(query, trends, top_k=6)
        print(relevant)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a trend analysis assistant. Use ONLY the provided trend data below. "
                    "If information is missing, say you don't know. Reply concisely in Korean with a natural, human tone."
                ),
            },
            {
                "role": "system",
                "content": context_text,
            },
            {
                "role": "system",
                "content": self._build_retrieval_context(relevant),
            },
        ] + user_messages

        stream = self.client.chat.completions.create(
            model=self.settings.model or "gpt-4o",
            messages=messages,
            stream=True
        )

        # reply = completion.choices[0].message.content or ""
        return stream, self._serialize_relevant(relevant)

    def _retrieve_relevant_items(self, query: str, trends: dict, top_k: int = 5) -> List[Tuple[float, dict]]:
        """
        사용자 질문을 임베딩해 트렌드 데이터와의 유사도가 높은 항목을 선별한다.
        임베딩 실패 시 빈 리스트 반환.
        """
        if not query or not self.embedding_service or not getattr(self.embedding_service, "client", None):
            return []

        candidates: List[dict] = []
        for bucket in ("popular", "rising", "recommended"):
            for item in trends.get(bucket, []) or []:
                enriched = dict(item)
                enriched["source"] = bucket
                candidates.append(enriched)
        for cat in trends.get("categories", []) or []:
            enriched = dict(cat)
            enriched["source"] = "category"
            candidates.append(enriched)

        if not candidates:
            return []

        texts = [self._item_text(c) for c in candidates]
        embeds = self.embedding_service.embed([query] + texts)
        if not embeds or len(embeds) != len(texts) + 1:
            return []

        query_vec = embeds[0]
        scored: List[Tuple[float, dict]] = []
        for emb, item in zip(embeds[1:], candidates):
            sim = cosine_similarity(query_vec, emb)
            scored.append((sim, item))

        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[:top_k]

    @staticmethod
    def _serialize_relevant(scored_items: List[Tuple[float, dict]]) -> list[dict]:
        """
        SSE 응답에 곁들일 수 있도록 간결한 형태로 직렬화한다.
        """
        payload: list[dict] = []
        for sim, item in scored_items:
            raw_data = {
                "source": item.get("source"),
                "video_id": item.get("video_id"),
                "title": item.get("title"),
                "channel_id": item.get("channel_id"),
                "platform": item.get("platform"),
                "view_count": item.get("view_count"),
                "like_count": item.get("like_count"),
                "comment_count": item.get("comment_count"),
                "published_at": item.get("published_at"),  # str() 안 해도 됨
                "thumbnail_url": item.get("thumbnail_url"),
                "category": item.get("category"),
                "sentiment_label": item.get("sentiment_label"),
                "sentiment_score": item.get("sentiment_score"),
                "trend_score": item.get("trend_score"),
                "engagement_score": item.get("engagement_score"),
                "score_sentiment": item.get("score_sentiment"),
                "score_trend": item.get("score_trend"),
                "total_score": item.get("total_score"),
                "crawled_at": item.get("crawled_at"),
                "channel_username": item.get("channel_username"),
                "similarity": round(sim, 3),
            }
            clean_data = jsonable_encoder(raw_data)
            payload.append(
                clean_data
            )
        return payload

    def _build_retrieval_context(self, scored_items: List[Tuple[float, dict]]) -> str:
        """
        임베딩으로 고른 상위 항목을 LLM에 컨텍스트로 제공한다.
        """
        if not scored_items:
            return "Relevant items: 없음 (임베딩 미사용)."

        lines: List[str] = []
        for sim, item in scored_items:
            parts = [
                f"source={item.get('source')}",
                f"title={item.get('title')}",
                f"category={item.get('category')}",
                f"views={item.get('view_count')}",
                f"score={item.get('total_score')}",
                f"sim={round(sim, 3)}",
            ]
            lines.append("- " + " | ".join(str(p) for p in parts if p is not None))

        return (
            "Top relevant items based on your question (use these as primary evidence):\n"
            + "\n".join(lines)
            + "\nRespond as if chatting with a person: include a brief recommendation reason and a short friendly closing."
        )

    @staticmethod
    def _fmt_video(item: dict) -> str:
        parts = [
            item.get("title") or "",
            f"views={item.get('view_count')}",
            f"channel={item.get('channel_id')}",
            f"category={item.get('category')}",
        ]
        return " | ".join(parts)

    @staticmethod
    def _item_text(item: dict) -> str:
        """
        임베딩 입력용 텍스트 구성 헬퍼.
        """
        parts = [
            item.get("title") or "",
            item.get("category") or "",
            item.get("summary") or "",
        ]
        return " ".join(p for p in parts if p)

    def _build_context(self, trends: dict) -> str:
        popular_items = trends.get("popular", [])[:5]
        rising_items = trends.get("rising", [])[:5]
        categories_items = trends.get("categories", [])[:5]
        recommended_items = trends.get("recommended", [])[:5]

        def fmt_list(items):
            return "\n".join(f"- {self._fmt_video(v)}" for v in items)

        popular = fmt_list(popular_items)
        rising = fmt_list(rising_items)
        categories = "\n".join(
            f"- {c.get('category')} (rank={c.get('rank')}, growth={c.get('growth_rate')})"
            for c in categories_items
            if c.get("category")
        )
        recommended = fmt_list(recommended_items)
        summary = trends.get("summary") or ""
        return (
            "Trend Data:\n"
            f"Popular:\n{popular}\n"
            f"Rising:\n{rising}\n"
            f"Categories:\n{categories}\n"
            f"Recommended:\n{recommended}\n"
            f"Summary: {summary}"
        )

    @staticmethod
    def _summarize_trends(categories: List[dict]) -> str:
        if not categories:
            return "트렌드 데이터가 부족합니다."
        top = categories[:3]
        lines = []
        for c in top:
            lines.append(
                f"{c.get('category')} (rank={c.get('rank')}, growth={c.get('growth_rate')})"
            )
        return "최근 주목받는 카테고리: " + "; ".join(lines)
