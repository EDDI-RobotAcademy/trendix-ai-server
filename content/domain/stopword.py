from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Stopword:
    id: Optional[int]
    lang: str
    word: str
    enabled: bool = True
    created_at: Optional[datetime] = None
