from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, desc
import json

from config.database.session import AsyncSessionLocal
from content.application.port.video_repository_port import VideoRepositoryPort
from content.domain.video_analysis import VideoAnalysisResult, TranscriptSegment, VisualFrame, DetectedObject
from content.infrastructure.orm.models import VideoAnalysisORM, VideoORM


class PostgresVideoRepository(VideoRepositoryPort):
    def __init__(self):
        self.session_factory = AsyncSessionLocal

    async def save_analysis(self, result: VideoAnalysisResult) -> None:
        async with self.session_factory() as session:
            # 기존 데이터가 있으면 삭제 (Upsert 효과)
            await session.execute(
                delete(VideoAnalysisORM).where(VideoAnalysisORM.video_id == result.video_id)
            )

            # transcript_timestamps 변환
            transcript_timestamps = [
                {
                    'start': seg.start,
                    'end': seg.end,
                    'text': seg.text
                }
                for seg in result.transcript_segments
            ]

            # visual_objects 변환
            visual_objects = [
                {
                    'timestamp': frame.timestamp,
                    'objects': [
                        {
                            'class_name': obj.class_name,
                            'confidence': obj.confidence
                        }
                        for obj in frame.objects
                    ]
                }
                for frame in result.visual_frames
            ]

            video_analysis = VideoAnalysisORM(
                video_id=result.video_id,
                transcript=result.transcript_text,
                transcript_timestamps=transcript_timestamps,
                subtitle_text=result.subtitle_data.get('text') if result.subtitle_data else None,
                key_topics=None,  # 추후 구현
                visual_objects=visual_objects,
                scene_changes=None,  # 추후 구현
                dominant_colors=None,  # 추후 구현
                analysis_completed_at=result.analyzed_at
            )

            session.add(video_analysis)
            await session.commit()

    async def get_analysis(self, video_id: str) -> Optional[VideoAnalysisResult]:
        async with self.session_factory() as session:
            # Join VideoAnalysisORM with VideoORM to get title and duration
            result = await session.execute(
                select(VideoAnalysisORM, VideoORM.title, VideoORM.duration)
                .join(VideoORM, VideoAnalysisORM.video_id == VideoORM.video_id, isouter=True)
                .where(VideoAnalysisORM.video_id == video_id)
                .order_by(desc(VideoAnalysisORM.created_at))
            )
            
            row = result.first()
            if not row:
                return None
            
            orm_obj, video_title, video_duration = row

            # ORM → Domain 모델 변환
            return VideoAnalysisResult(
                video_id=orm_obj.video_id,
                video_title=video_title,
                video_duration=video_duration,
                transcript_text=orm_obj.transcript,
                transcript_segments=[
                    TranscriptSegment(**seg)
                    for seg in orm_obj.transcript_timestamps or []
                ],
                visual_frames=[
                    VisualFrame(
                        timestamp=frame['timestamp'],
                        objects=[DetectedObject(**obj) for obj in frame['objects']]
                    )
                    for frame in orm_obj.visual_objects or []
                ],
                subtitle_data={'text': orm_obj.subtitle_text} if orm_obj.subtitle_text else None,
                analyzed_at=orm_obj.analysis_completed_at
            )