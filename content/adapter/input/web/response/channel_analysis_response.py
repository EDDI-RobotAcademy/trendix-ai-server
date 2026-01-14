from pydantic import BaseModel


class ChannelVideoResponse(BaseModel):
    id: str
    title: str
    thumbnailUrl: str | None
    viewCount: int
    trendAvgViewCount: int
    publishedAt: str
    performance: str


class ChannelAnalysisResponse(BaseModel):
    channelId: str
    channelName: str
    channelThumbnail: str | None
    subscriberCount: int
    totalVideos: int
    overallPerformance: int
    recentVideos: list[ChannelVideoResponse]
