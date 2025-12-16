import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class OpenAISettings:
    api_key: str = os.getenv("OPENAI_API_KEY", "")
    model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


@dataclass
class YouTubeSettings:
    api_key: str = os.getenv("YOUTUBE_API_KEY", "")
    quota_user: str | None = os.getenv("YOUTUBE_QUOTA_USER")


@dataclass
class TikTokSettings:
    api_key: str = os.getenv("TIKTOK_API_KEY", "")
    base_url: str = os.getenv("TIKTOK_BASE_URL", "")


@dataclass
class InstagramSettings:
    access_token: str = os.getenv("INSTAGRAM_ACCESS_TOKEN", "")
    app_id: str = os.getenv("INSTAGRAM_APP_ID", "")
    base_url: str = os.getenv("INSTAGRAM_BASE_URL", "")
