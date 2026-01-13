from config.database.session import AsyncSessionLocal
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from typing import List, Dict

from content.application.port.embedding_repository_port import EmbeddingRepositoryPort
from content.domain.embedding import EmbeddingData
from content.infrastructure.orm.models import VideoEmbeddingORM


class PostgresEmbeddingRepository(EmbeddingRepositoryPort):
    def __init__(self):
        self.session_factory = AsyncSessionLocal

    async def save_embeddings(self, embeddings: List[EmbeddingData]) -> None:
        if not embeddings:
            return

        video_id = embeddings[0].video_id
        async with self.session_factory() as session:
            # 기존 데이터가 있으면 삭제 (Upsert 효과)
            from sqlalchemy import delete
            await session.execute(
                delete(VideoEmbeddingORM).where(VideoEmbeddingORM.video_id == video_id)
            )

            for emb in embeddings:
                embedding_orm = VideoEmbeddingORM(
                    video_id=emb.video_id,
                    chunk_type=emb.chunk_type,
                    chunk_text=emb.chunk_text,
                    chunk_metadata=emb.chunk_metadata,
                    embedding=emb.embedding
                )
                session.add(embedding_orm)

            await session.commit()

    async def search_similar(self, query_embedding: List[float], limit: int = 10) -> List[Dict]:
        async with self.session_factory() as session:
            # pgvector의 코사인 유사도 검색
            query = text("""
                SELECT 
                    video_id,
                    chunk_type,
                    chunk_text,
                    chunk_metadata,
                    1 - (embedding <=> :query_embedding) as similarity
                FROM video_embeddings
                ORDER BY embedding <=> :query_embedding
                LIMIT :limit
            """)

            result = await session.execute(
                query,
                {
                    'query_embedding': str(query_embedding),
                    'limit': limit
                }
            )

            return [
                {
                    'video_id': row[0],
                    'chunk_type': row[1],
                    'chunk_text': row[2],
                    'chunk_metadata': row[3],
                    'similarity': float(row[4])
                }
                for row in result.fetchall()
            ]