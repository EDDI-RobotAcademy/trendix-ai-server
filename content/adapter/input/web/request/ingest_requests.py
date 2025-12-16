from pydantic import BaseModel, Field


class IngestChannelRequest(BaseModel):
    include_comments: bool = Field(default=False, description="Whether to fetch comments for each video")
    max_videos: int = Field(default=10, ge=1, le=50)
    max_comments: int = Field(default=50, ge=1, le=100)


class IngestVideoRequest(BaseModel):
    include_comments: bool = Field(default=True, description="Whether to fetch comments")
    max_comments: int = Field(default=50, ge=1, le=100)
