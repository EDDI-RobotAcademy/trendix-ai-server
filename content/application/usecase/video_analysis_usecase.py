from datetime import datetime
from typing import Optional

from content.application.port.object_detection_port import ObjectDetectionPort
from content.application.port.stt_service_port import STTServicePort
from content.application.port.video_downloader_port import VideoDownloader
from content.application.port.video_repository_port import VideoRepositoryPort
from content.domain.video_analysis import VideoAnalysisResult, TranscriptSegment
from content.infrastructure.service.embedding_service import EmbeddingService


class VideoAnalysisUseCase:
    def __init__(
            self,
            video_downloader: VideoDownloader,
            stt_service: STTServicePort,
            object_detection: ObjectDetectionPort,
            video_repository: VideoRepositoryPort,
            embedding_service: Optional['EmbeddingService'] = None
    ):
        self.video_downloader = video_downloader
        self.stt_service = stt_service
        self.object_detection = object_detection
        self.video_repository = video_repository
        self.embedding_service = embedding_service

    async def analyze_video(self, video_id: str, video_url: str) -> VideoAnalysisResult:
        video_path = None
        try:
            # 1. 영상 다운로드
            video_path = await self.video_downloader.download(video_url)

            # 2. STT 처리
            transcript_data = await self.stt_service.transcribe(video_path)

            # 3. 객체 감지
            visual_frames = await self.object_detection.detect_objects(video_path)

            # 4. 결과 구성
            result = VideoAnalysisResult(
                video_id=video_id,
                transcript_text=transcript_data['text'],
                transcript_segments=[
                    TranscriptSegment(
                        start=seg['start'],
                        end=seg['end'],
                        text=seg['text']
                    ) for seg in transcript_data['segments']
                ],
                visual_frames=visual_frames,
                subtitle_data=None,  # 추후 구현
                analyzed_at=datetime.now()
            )
            print(result)
            # 5. 저장
            await self.video_repository.save_analysis(result)

            print("여기 옵니다.")
            # 6. 임베딩 생성 (비동기)
            if self.embedding_service:
                await self.embedding_service.generate_embeddings(video_id)

            return result

        finally:
            # 7. 정리
            if video_path:
                await self.video_downloader.cleanup(video_path)