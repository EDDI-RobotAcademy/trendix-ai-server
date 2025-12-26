from __future__ import annotations

from typing import List

from openai import OpenAI

from config.settings import OpenAISettings
from content.application.usecase.trend_featured_usecase import TrendFeaturedUseCase


class TrendChatUseCase:
    """
    트렌드 데이터를 컨텍스트로 주입한 챗 응답을 생성하는 유스케이스.
    - 인기/급상승/카테고리/추천(질의 기반 재정렬)을 LLM에 넣어 답변을 생성한다.
    """

    def __init__(
        self,
        featured_usecase: TrendFeaturedUseCase,
        settings: OpenAISettings | None = None,
    ):
        self.featured_usecase = featured_usecase
        self.settings = settings or OpenAISettings()
        if not self.settings.api_key:
            raise ValueError("OPENAI_API_KEY is required for TrendChatUseCase")
        self.client = OpenAI(api_key=self.settings.api_key)

    def answer_with_trends(
        self,
        user_messages: List[dict],
        popular_limit: int = 5,
        rising_limit: int = 5,
        velocity_days: int = 1,
        platform: str | None = None,
    ) -> str:
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
            return "트렌드 데이터가 부족해요. 나중에 다시 시도해 주세요."

        context_text = self._build_context(trends)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a trend analysis assistant. Use ONLY the provided trend data below. "
                    "If information is missing, say you don't know. Reply concisely in Korean."
                ),
            },
            {
                "role": "system",
                "content": context_text,
            },
        ] + user_messages

        completion = self.client.chat.completions.create(
            model=self.settings.model or "gpt-4o",
            messages=messages,
        )
        return completion.choices[0].message.content or ""

    @staticmethod
    def _fmt_video(item: dict) -> str:
        parts = [
            item.get("title") or "",
            f"views={item.get('view_count')}",
            f"channel={item.get('channel_id')}",
            f"category={item.get('category')}",
        ]
        return " | ".join(parts)

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
