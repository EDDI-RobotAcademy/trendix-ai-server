import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

from ..core.interfaces import SchedulerInterface, SchedulerObserverInterface, SchedulerStatus
from ..core.events import SchedulerEvent, CollectionResult, TrendAnalysisResult
from ..config.scheduler_config import SchedulerConfig, CollectionStrategy


logger = logging.getLogger(__name__)


class SchedulerObserver(SchedulerObserverInterface):
    """기본 스케줄러 관찰자 구현"""
    
    def __init__(self, manager_name: str = "default"):
        self.manager_name = manager_name
        
    async def on_collection_completed(self, result: CollectionResult) -> None:
        """영상 수집 완료 이벤트 처리"""
        logger.info(f"[{self.manager_name}] Collection completed: {result.total_collected} videos collected")
        
    async def on_analysis_completed(self, result: TrendAnalysisResult) -> None:
        """트렌드 분석 완료 이벤트 처리"""
        trending_count = len(result.trending_videos)
        category_count = len(result.category_trends)
        logger.info(f"[{self.manager_name}] Analysis completed: {trending_count} trending videos, {category_count} categories")
        
    async def on_error(self, error: Exception) -> None:
        """에러 이벤트 처리"""
        logger.error(f"[{self.manager_name}] Scheduler error: {error}")


class SchedulerManager:
    """스케줄러 매니저 - 여러 스케줄러 인스턴스를 관리"""
    
    def __init__(self, manager_name: str = "scheduler_manager"):
        self.manager_name = manager_name
        self._schedulers: Dict[str, SchedulerInterface] = {}
        self._observer = SchedulerObserver(manager_name)
        self._shutdown_event = asyncio.Event()
        
    def register_scheduler(self, scheduler: SchedulerInterface, scheduler_id: str) -> None:
        """스케줄러 등록"""
        if scheduler_id in self._schedulers:
            logger.warning(f"Scheduler {scheduler_id} already registered, replacing")
            
        self._schedulers[scheduler_id] = scheduler
        scheduler.add_observer(self._observer)
        logger.info(f"Registered scheduler: {scheduler_id}")
        
    def unregister_scheduler(self, scheduler_id: str) -> None:
        """스케줄러 등록 해제"""
        if scheduler_id in self._schedulers:
            scheduler = self._schedulers[scheduler_id]
            scheduler.remove_observer(self._observer)
            del self._schedulers[scheduler_id]
            logger.info(f"Unregistered scheduler: {scheduler_id}")
            
    async def start_scheduler(self, scheduler_id: str) -> None:
        """특정 스케줄러 시작"""
        if scheduler_id not in self._schedulers:
            raise ValueError(f"Scheduler {scheduler_id} not found")
            
        scheduler = self._schedulers[scheduler_id]
        await scheduler.start()
        logger.info(f"Started scheduler: {scheduler_id}")
        
    async def stop_scheduler(self, scheduler_id: str) -> None:
        """특정 스케줄러 정지"""
        if scheduler_id not in self._schedulers:
            logger.warning(f"Scheduler {scheduler_id} not found")
            return
            
        scheduler = self._schedulers[scheduler_id]
        await scheduler.stop()
        logger.info(f"Stopped scheduler: {scheduler_id}")
        
    async def start_all(self) -> None:
        """모든 스케줄러 시작"""
        start_tasks = []
        for scheduler_id, scheduler in self._schedulers.items():
            if scheduler.get_status() == SchedulerStatus.STOPPED:
                start_tasks.append(self.start_scheduler(scheduler_id))
                
        if start_tasks:
            await asyncio.gather(*start_tasks, return_exceptions=True)
            logger.info(f"Started {len(start_tasks)} schedulers")
            
    async def stop_all(self) -> None:
        """모든 스케줄러 정지"""
        stop_tasks = []
        for scheduler_id, scheduler in self._schedulers.items():
            if scheduler.get_status() != SchedulerStatus.STOPPED:
                stop_tasks.append(self.stop_scheduler(scheduler_id))
                
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
            logger.info(f"Stopped {len(stop_tasks)} schedulers")
            
        self._shutdown_event.set()
        
    def get_scheduler_status(self, scheduler_id: str) -> Optional[SchedulerStatus]:
        """특정 스케줄러 상태 조회"""
        if scheduler_id not in self._schedulers:
            return None
        return self._schedulers[scheduler_id].get_status()
        
    def get_all_statuses(self) -> Dict[str, SchedulerStatus]:
        """모든 스케줄러 상태 조회"""
        return {
            scheduler_id: scheduler.get_status() 
            for scheduler_id, scheduler in self._schedulers.items()
        }
        
    def list_schedulers(self) -> List[str]:
        """등록된 스케줄러 ID 목록"""
        return list(self._schedulers.keys())
        
    async def wait_for_shutdown(self) -> None:
        """종료 대기"""
        await self._shutdown_event.wait()


class SchedulerFactory:
    """스케줄러 팩토리 클래스"""
    
    @staticmethod
    def create_scheduler(config: SchedulerConfig) -> SchedulerInterface:
        """설정에 따라 적절한 스케줄러 생성"""
        from ..core.base_scheduler import BaseScheduler
        from ..strategies.selective_video_strategy import SelectiveVideoCollectionStrategy
        from ..analyzers.trend_analyzer import CompositeTrendAnalyzer
        
        # 전략 패턴에 따른 수집 전략 생성
        collector = SchedulerFactory._create_collector(config)
        
        # 트렌드 분석기 생성
        analyzer = CompositeTrendAnalyzer(config.analysis_config)
        
        # 스케줄러 생성
        scheduler = BaseScheduler(
            scheduler_id=config.scheduler_id,
            collector=collector,
            analyzer=analyzer,
            interval_minutes=config.interval_minutes
        )
        
        return scheduler
    
    @staticmethod
    def _create_collector(config: SchedulerConfig):
        """수집 전략에 따른 컬렉터 생성"""
        if config.collection_strategy == CollectionStrategy.SELECTIVE_VIDEOS:
            from ..strategies.selective_video_strategy import SelectiveVideoCollectionStrategy
            return SelectiveVideoCollectionStrategy(config.collection_config)
        elif config.collection_strategy == CollectionStrategy.TRENDING_VIDEOS:
            # 향후 확장을 위한 다른 전략들
            from ..strategies.selective_video_strategy import SelectiveVideoCollectionStrategy
            return SelectiveVideoCollectionStrategy(config.collection_config)
        else:
            # 기본값은 selective 전략
            from ..strategies.selective_video_strategy import SelectiveVideoCollectionStrategy
            return SelectiveVideoCollectionStrategy(config.collection_config)
    
    @staticmethod
    def create_mvp_scheduler(scheduler_id: str = "mvp_youtube_scheduler") -> SchedulerInterface:
        """MVP 요구사항에 맞는 스케줄러 생성"""
        from ..config.scheduler_config import SchedulerConfigFactory
        
        config = SchedulerConfigFactory.create_mvp_config(scheduler_id)
        return SchedulerFactory.create_scheduler(config)
    
    @staticmethod
    def create_from_env(scheduler_id: str) -> SchedulerInterface:
        """환경변수 기반 스케줄러 생성"""
        from ..config.scheduler_config import SchedulerConfigFactory
        
        config = SchedulerConfigFactory.create_from_env(scheduler_id)
        return SchedulerFactory.create_scheduler(config)