from pydantic import BaseModel


class VideoHistoryPoint(BaseModel):
    time: str
    count: int


class VideoDetailResponse(BaseModel):
    id: str
    title: str
    description: str
    channelName: str
    channelId: str
    channelThumbnail: str | None
    thumbnailUrl: str | None
    viewCount: int
    likeCount: int
    commentCount: int
    publishedAt: str
    duration: str
    categoryId: str
    categoryName: str
    tags: list[str]
    isShort: bool
    trendingRank: int | None = None
    trendingReason: str | None = None
    viewHistory: list[VideoHistoryPoint]
    likeHistory: list[VideoHistoryPoint]
