from pydantic import BaseModel


class VideoCoreResponse(BaseModel):
    id: str | None
    title: str
    channel_name: str
    thumbnail_url: str | None = None
    duration_sec: int
    format_label: str
    published_ago: str


class HookProfileResponse(BaseModel):
    opening_line: str
    visual_cue: str
    caption_style: str
    pacing: str
    hook_score: int


class FormatProfileResponse(BaseModel):
    duration_sec: int
    aspect_ratio: str
    cut_count: int
    text_density: str
    audio_style: str


class ReactionMetricsResponse(BaseModel):
    views: int
    likes: int
    comments: int
    like_rate: float
    completion_rate: float
    retention_3s: float
    share_rate: float


class ShortsCompareResponse(BaseModel):
    my_video: VideoCoreResponse
    trend_video: VideoCoreResponse
    hook_comparison: dict
    format_comparison: dict
    reaction_comparison: dict
    ai_summary: dict
    trust_signals: list[str]
