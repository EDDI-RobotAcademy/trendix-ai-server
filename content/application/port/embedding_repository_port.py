from abc import ABC, abstractmethod
from typing import List, Dict

from content.domain.embedding import EmbeddingData


class EmbeddingRepositoryPort(ABC):
    @abstractmethod
    async def save_embeddings(self, embeddings: List[EmbeddingData]) -> None:
        pass

    @abstractmethod
    async def search_similar(self, query_embedding: List[float], limit: int = 10) -> List[Dict]:
        pass