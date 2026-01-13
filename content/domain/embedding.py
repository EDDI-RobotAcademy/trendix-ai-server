from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class ChunkData:
    chunk_type: str
    text: str
    metadata: Dict


@dataclass
class EmbeddingData:
    video_id: str
    chunk_type: str
    chunk_text: str
    chunk_metadata: Dict
    embedding: List[float]