from pydantic import BaseModel, Field


class ShortsCompareRequest(BaseModel):
    platform: str = Field(default="youtube", description="플랫폼 (현재 youtube만 지원)")
    my_short_url: str = Field(..., description="내 쇼츠 URL")
    trend_short_url: str = Field(..., description="급등 쇼츠 URL")
