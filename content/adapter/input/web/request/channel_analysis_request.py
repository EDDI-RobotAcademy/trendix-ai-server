from pydantic import BaseModel, Field


class ChannelAnalysisRequest(BaseModel):
    platform: str = Field(default="youtube")
    channel_url: str = Field(min_length=1)
    limit: int = Field(default=6, ge=1, le=20)
    trend_days: int = Field(default=14, ge=1, le=90)
