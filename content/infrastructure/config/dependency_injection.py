import os
from dotenv import load_dotenv
from dependency_injector import containers, providers

from content.adapter.output.http_video_downloader import HTTPVideoDownloader
from content.adapter.output.sentence_transformer_adapter import SentenceTransformerEmbeddingAdapter
from content.adapter.output.whisper_stt_adapter import WhisperSTTAdapter
from content.adapter.output.yolo_detection_adapter import YOLODetectionAdapter
from content.application.usecase.video_analysis_usecase import VideoAnalysisUseCase
from content.application.usecase.guide_chat_usecase import GuideChatUseCase
from content.infrastructure.repository.postgres_embedding_repository_impl import PostgresEmbeddingRepository
from content.infrastructure.repository.postgres_video_repository_impl import PostgresVideoRepository
from content.infrastructure.service.embedding_service import EmbeddingService


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    # Adapters
    video_downloader = providers.Singleton(
        HTTPVideoDownloader,
        temp_dir=config.temp_dir
    )

    stt_service = providers.Singleton(
        WhisperSTTAdapter,
        model_name=config.whisper_model
    )

    object_detection = providers.Singleton(
        YOLODetectionAdapter,
        model_path=config.yolo_model
    )

    video_repository = providers.Singleton(
        PostgresVideoRepository
    )

    embedding_generator = providers.Singleton(
        SentenceTransformerEmbeddingAdapter,
        model_name=config.embedding_model,
        use_gpu=config.use_gpu
    )

    embedding_repository = providers.Singleton(
        PostgresEmbeddingRepository
    )

    # Services
    embedding_service = providers.Factory(
        EmbeddingService,
        embedding_generator=embedding_generator,
        embedding_repository=embedding_repository,
        video_repository=video_repository,
        target_chunk_duration=config.target_chunk_duration,
        scene_change_threshold=config.scene_change_threshold
    )

    video_analysis_service = providers.Factory(
        VideoAnalysisUseCase,
        video_downloader=video_downloader,
        stt_service=stt_service,
        object_detection=object_detection,
        video_repository=video_repository,
        embedding_service=embedding_service
    )

    guide_chat_usecase = providers.Factory(
        GuideChatUseCase,
        embedding_generator=embedding_generator,
        embedding_repository=embedding_repository,
        video_repository=video_repository
    )


def create_container() -> Container:
    load_dotenv()
    
    container = Container()
    container.config.from_dict({
        'temp_dir': '/tmp/videos',
        'whisper_model': 'small',
        'yolo_model': 'yolov8n.pt',
        'embedding_model': 'paraphrase-multilingual-mpnet-base-v2',
        'use_gpu': True,
        'target_chunk_duration': 7.0,
        'scene_change_threshold': 0.3
    })
    return container