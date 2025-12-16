from dataclasses import dataclass
from typing import Optional


@dataclass
class KeywordMapping:
    mapping_id: Optional[int]
    video_id: Optional[str]
    channel_id: Optional[str]
    platform: Optional[str]
    keyword: str
    weight: Optional[float] = None
