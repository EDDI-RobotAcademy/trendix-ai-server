from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CreatorAccount:
    account_id: str
    platform: str
    display_name: str
    username: Optional[str] = None
    profile_url: Optional[str] = None
    description: Optional[str] = None
    country: Optional[str] = None
    follower_count: Optional[int] = None
    post_count: Optional[int] = None
    last_updated_at: Optional[datetime] = None
    crawled_at: Optional[datetime] = None
