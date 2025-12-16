import json
from typing import Iterable

from openai import OpenAI

from config.settings import OpenAISettings
from content.domain.comment_sentiment import CommentSentiment
from content.domain.video import Video
from content.domain.video_comment import VideoComment
from content.domain.video_sentiment import VideoSentiment


class SentimentUseCase:
    def __init__(self, settings: OpenAISettings):
        self.settings = settings
        self.client = OpenAI(api_key=settings.api_key)

    def analyze_video(self, video: Video) -> VideoSentiment:
        prompt = (
            "You are analyzing sentiment for a YouTube video.\n"
            "Return a JSON object with keys: "
            "category (short category label), "
            "trend_score (0-1 float; how trending/popular the topic feels), "
            "sentiment_label (positive|neutral|negative), "
            "sentiment_score (0-1 float), "
            "keywords (comma separated), "
            "summary (short sentence).\n"
            f"Title: {video.title}\nDescription: {video.description or ''}\nTags: {video.tags or ''}"
        )
        payload = self._request_json(prompt)
        return VideoSentiment(
            video_id=video.video_id,
            category=payload.get("category"),
            trend_score=float(payload.get("trend_score", 0.0)),
            sentiment_label=payload.get("sentiment_label"),
            sentiment_score=float(payload.get("sentiment_score", 0.0)),
            keywords=payload.get("keywords"),
            summary=payload.get("summary"),
        )

    def analyze_comments(self, comments: Iterable[VideoComment]) -> list[CommentSentiment]:
        sentiments: list[CommentSentiment] = []
        for comment in comments:
            prompt = (
                "You are analyzing sentiment for a YouTube comment.\n"
                "Return a JSON object with keys sentiment_label (positive|neutral|negative) "
                "and sentiment_score (0-1 float).\n"
                f"Comment text: {comment.content}"
            )
            payload = self._request_json(prompt)
            sentiments.append(
                CommentSentiment(
                    comment_id=comment.comment_id,
                    sentiment_label=payload.get("sentiment_label"),
                    sentiment_score=float(payload.get("sentiment_score", 0.0)),
                )
            )
        return sentiments

    def _request_json(self, prompt: str) -> dict:
        response = self.client.chat.completions.create(
            model=self.settings.model,
            messages=[{"role": "system", "content": "Return valid JSON only."}, {"role": "user", "content": prompt}],
            temperature=0,
        )
        content = response.choices[0].message.content or "{}"
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {}
