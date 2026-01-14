from typing import List
from sentence_transformers import SentenceTransformer
import gc
import torch

from content.application.port.embedding_generator_port import EmbeddingGeneratorPort


class SentenceTransformerEmbeddingAdapter(EmbeddingGeneratorPort):
    def __init__(self, model_name: str = "jhgan/ko-sroberta-multitask", use_gpu: bool = True):
        # GPU 사용 가능 여부 확인 및 디바이스 설정
        self.device = "cuda" if use_gpu and torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(
        model_name, 
        device=self.device,
        trust_remote_code=True,
        model_kwargs={
            'use_safetensors': True  # safetensors 사용
        }
    )
        print(f"SentenceTransformer 모델({model_name})이 {self.device} 디바이스에서 로드되었습니다.")

    async def generate_embedding(self, text: str) -> List[float]:
        try:
            if self.device == "cuda":
                self.model.to(self.device)
                
            embedding = self.model.encode(text)
            return embedding.tolist()
        finally:
            if self.device == "cuda":
                self.model.to("cpu")
                gc.collect()
                torch.cuda.empty_cache()

    async def generate_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        try:
            if self.device == "cuda":
                self.model.to(self.device)
                
            embeddings = self.model.encode(texts)
            return embeddings.tolist()
        finally:
            if self.device == "cuda":
                self.model.to("cpu")
                gc.collect()
                torch.cuda.empty_cache()
