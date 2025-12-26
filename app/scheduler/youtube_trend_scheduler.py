"""
YouTube 트렌드 스케줄러 메인 모듈
"""
import asyncio
import os
import logging
from typing import Optional

from .managers.scheduler_manager import SchedulerManager, SchedulerFactory


logger = logging.getLogger(__name__)


class YouTubeTrendSchedulerService:
    """YouTube 트렌드 스케줄러 서비스"""
    
    def __init__(self):
        self.manager = SchedulerManager("youtube_trend_manager")
        self._is_initialized = False
        
    async def initialize(self) -> None:
        """스케줄러 서비스 초기화"""
        if self._is_initialized:
            return
            
        try:
            # MVP 스케줄러 생성 및 등록
            mvp_scheduler = SchedulerFactory.create_mvp_scheduler("mvp_youtube_scheduler")
            self.manager.register_scheduler(mvp_scheduler, "mvp_youtube_scheduler")
            
            # 추가 스케줄러가 필요한 경우 여기서 등록
            # 환경변수 기반 스케줄러도 생성 가능
            if os.getenv("ENABLE_CUSTOM_SCHEDULER", "false").lower() == "true":
                custom_scheduler = SchedulerFactory.create_from_env("custom_youtube_scheduler")
                self.manager.register_scheduler(custom_scheduler, "custom_youtube_scheduler")
            
            self._is_initialized = True
            logger.info("YouTube trend scheduler service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize scheduler service: {e}")
            raise
            
    async def start(self) -> None:
        """스케줄러 서비스 시작"""
        if not self._is_initialized:
            await self.initialize()
            
        await self.manager.start_all()
        logger.info("YouTube trend scheduler service started")
        
    async def stop(self) -> None:
        """스케줄러 서비스 정지"""
        await self.manager.stop_all()
        logger.info("YouTube trend scheduler service stopped")
        
    def get_manager(self) -> SchedulerManager:
        """매니저 인스턴스 반환 (상태 조회용)"""
        return self.manager


# 전역 인스턴스
_scheduler_service: Optional[YouTubeTrendSchedulerService] = None


def get_scheduler_service() -> YouTubeTrendSchedulerService:
    """스케줄러 서비스 싱글톤 인스턴스 반환"""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = YouTubeTrendSchedulerService()
    return _scheduler_service


async def start_youtube_trend_scheduler() -> None:
    """
    YouTube 트렌드 스케줄러 시작 (FastAPI lifespan에서 호출)
    """
    # 환경변수로 비활성화 가능
    if os.getenv("ENABLE_YOUTUBE_TREND_SCHEDULER", "true").lower() != "true":
        logger.info("YouTube trend scheduler disabled by environment variable")
        return
        
    try:
        service = get_scheduler_service()
        await service.start()
    except Exception as e:
        logger.error(f"Failed to start YouTube trend scheduler: {e}")
        raise


async def stop_youtube_trend_scheduler() -> None:
    """
    YouTube 트렌드 스케줄러 정지 (FastAPI lifespan에서 호출)
    """
    global _scheduler_service
    if _scheduler_service:
        try:
            await _scheduler_service.stop()
        except Exception as e:
            logger.error(f"Failed to stop YouTube trend scheduler: {e}")
        finally:
            _scheduler_service = None