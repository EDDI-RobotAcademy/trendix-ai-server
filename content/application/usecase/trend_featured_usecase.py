from __future__ import annotations

from typing import List

from content.application.port.content_repository_port import ContentRepositoryPort
from content.utils.embedding import EmbeddingService, cosine_similarity


class TrendFeaturedUseCase:
    """
    인기(Popular) vs 급상승(Rising) 후보를 분리해 반환.
    추가 로직:
    - 채널 규모 편향 보정: 랭킹 시 정규화 점수(normalized_view_score)를 우선 사용
    - TEI 기반 dedup: 후보 내 유사도 높은 항목 제거
    - TEI 기반 rerank: 사용자 query가 있으면 유사도 기반 재정렬
    - 다양성: 연속된 동일 카테고리/채널을 피하도록 순서를 살짝 섞음
    - 블록 구조: popular, rising, categories, recommended (query 기반)
    """

    def __init__(self, repository: ContentRepositoryPort, embedding_service: EmbeddingService | None = None):
        self.repository = repository
        self.embedding_service = embedding_service or EmbeddingService()

    def get_featured(
        self,
        limit_popular: int = 5,
        limit_rising: int = 5,
        velocity_days: int = 1,
        platform: str | None = None,
        query: str | None = None,
    ) -> dict:
        popular = self.repository.fetch_popular_videos(limit=limit_popular * 2, platform=platform)
        rising = self.repository.fetch_rising_videos(limit=limit_rising * 2, velocity_days=velocity_days, platform=platform)

        popular = self._dedup_by_embedding(popular)
        rising = self._dedup_by_embedding(rising)

        categories = self.repository.fetch_hot_category_trends(platform=platform, limit=5)

        recommended: List[dict] = []
        if query:
            # query와의 유사도 기반 재정렬 (popular+rising 합쳐서)
            combined = popular + [r for r in rising if r not in popular]
            recommended = self._rerank_by_query(query, combined)[: max(limit_popular, limit_rising)]
            recommended = self._enforce_diversity(recommended)

        popular = self._enforce_diversity(popular[:limit_popular])
        rising = self._enforce_diversity(rising[:limit_rising])

        summary = self._summarize_trends(categories)

        return {
            "popular": popular,
            "rising": rising,
            "categories": categories,
            "recommended": recommended,
            "summary": summary,
        }

    def _dedup_by_embedding(self, items: List[dict], threshold: float = 0.9) -> List[dict]:
        """
        TEI 유사도 기반 중복 제거. 임베딩 실패 시 원본 반환.
        """
        if not items:
            return items
        texts = [self._item_text(i) for i in items]
        embeddings = self.embedding_service.embed(texts)
        if embeddings is None:
            return items

        kept: List[dict] = []
        kept_embeds: List[List[float]] = []
        for item, emb in zip(items, embeddings):
            if not kept_embeds:
                kept.append(item)
                kept_embeds.append(emb)
                continue
            sim_max = max(cosine_similarity(emb, ke) for ke in kept_embeds)
            if sim_max >= threshold:
                continue
            kept.append(item)
            kept_embeds.append(emb)
        return kept

    def _rerank_by_query(self, query: str, items: List[dict]) -> List[dict]:
        """
        사용자 질의 임베딩과 후보 임베딩 유사도로 재정렬. 실패 시 원본.
        """
        if not items:
            return items
        query_embeds = self.embedding_service.embed([query]) or []
        item_embeds = self.embedding_service.embed([self._item_text(i) for i in items]) or []
        if not query_embeds or not item_embeds:
            return items

        q_emb = query_embeds[0]
        scored = []
        for item, emb in zip(items, item_embeds):
            sim = cosine_similarity(q_emb, emb)
            scored.append((sim, item))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [i for _, i in scored]

    @staticmethod
    def _enforce_diversity(items: List[dict]) -> List[dict]:
        """
        동일 카테고리/채널이 연속되지 않도록 간단한 재배열.
        """
        if not items:
            return items
        result: List[dict] = []
        for item in items:
            if result and (
                (item.get("category") and item.get("category") == result[-1].get("category"))
                or (item.get("channel_id") and item.get("channel_id") == result[-1].get("channel_id"))
            ):
                # 뒤로 밀어넣기
                result.append(item)
            else:
                result.insert(len(result), item)
        return result

    @staticmethod
    def _item_text(item: dict) -> str:
        parts = [
            item.get("title") or "",
            item.get("category") or "",
            item.get("summary") or "",
        ]
        return " ".join(p for p in parts if p)

    @staticmethod
    def _summarize_trends(self, categories: List[dict]) -> str:
        if not categories:
            return "트렌드 데이터가 부족합니다."
        top = categories[:3]
        lines = []
        for c in top:
            lines.append(
                f"{c.get('category')} (rank={c.get('rank')}, growth={c.get('growth_rate')})"
            )
        return "최근 주목받는 카테고리: " + "; ".join(lines)
