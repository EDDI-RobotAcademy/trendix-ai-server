import math
from typing import Iterable, List, Sequence

from openai import OpenAI

from config.settings import OpenAISettings

# 기본 TEI 모델 (공개 임베딩)
EMBED_MODEL = "text-embedding-3-small"


class EmbeddingService:
    """
    OpenAI 임베딩 헬퍼.
    - API 키가 없을 경우 None 반환하여 상위 로직에서 graceful degrade 할 수 있게 한다.
    """

    def __init__(self, settings: OpenAISettings | None = None):
        self.settings = settings or OpenAISettings()
        if self.settings.api_key:
            self.client = OpenAI(api_key=self.settings.api_key)
        else:
            self.client = None

    def embed(self, texts: Sequence[str]) -> List[List[float]] | None:
        if not self.client:
            return None
        # 임베딩 모델은 고정(TEI 기본 공개 모델 사용)
        resp = self.client.embeddings.create(model=EMBED_MODEL, input=list(texts))
        return [d.embedding for d in resp.data]


def cosine_similarity(vec_a: Iterable[float], vec_b: Iterable[float]) -> float:
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
