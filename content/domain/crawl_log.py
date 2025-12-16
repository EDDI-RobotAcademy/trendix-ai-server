from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CrawlLog:
    id: Optional[int]
    target_type: str
    target_id: str
    status: str
    message: Optional[str] = None
    crawled_at: Optional[datetime] = None
