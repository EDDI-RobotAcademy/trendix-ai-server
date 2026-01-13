from abc import ABC, abstractmethod
from typing import List


class EmbeddingGeneratorPort(ABC):
    @abstractmethod
    async def generate_embedding(self, text: str) -> List[float]:
        pass

    @abstractmethod
    async def generate_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        pass