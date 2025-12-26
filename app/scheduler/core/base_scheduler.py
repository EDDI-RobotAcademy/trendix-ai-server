import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Set

from .interfaces import (
    SchedulerInterface, 
    VideoCollectorInterface, 
    TrendAnalyzerInterface,
    SchedulerObserverInterface,
    SchedulerStatus
)
from .events import (
    SchedulerEvent, 
    CollectionStartedEvent, 
    CollectionCompletedEvent,
    AnalysisStartedEvent,
    AnalysisCompletedEvent,
    ErrorEvent
)


logger = logging.getLogger(__name__)


class BaseScheduler(SchedulerInterface):
    """스케줄러 베이스 클래스"""
    
    def __init__(
        self,
        scheduler_id: str,
        collector: VideoCollectorInterface,
        analyzer: TrendAnalyzerInterface,
        interval_minutes: int = 30
    ):
        self.scheduler_id = scheduler_id
        self.collector = collector
        self.analyzer = analyzer
        self.interval_minutes = interval_minutes
        self.interval_seconds = interval_minutes * 60
        
        self._status = SchedulerStatus.STOPPED
        self._task: Optional[asyncio.Task] = None
        self._observers: Set[SchedulerObserverInterface] = set()
        self._shutdown_event = asyncio.Event()
        
    def add_observer(self, observer: SchedulerObserverInterface) -> None:
        """관찰자 추가"""
        self._observers.add(observer)
        
    def remove_observer(self, observer: SchedulerObserverInterface) -> None:
        """관찰자 제거"""
        self._observers.discard(observer)
        
    async def _notify_observers(self, event: SchedulerEvent) -> None:
        """관찰자들에게 이벤트 알림"""
        for observer in self._observers:
            try:
                if hasattr(event, 'result') and hasattr(event.result, '__class__'):
                    if 'CollectionResult' in str(event.result.__class__):
                        await observer.on_collection_completed(event.result)
                    elif 'TrendAnalysisResult' in str(event.result.__class__):
                        await observer.on_analysis_completed(event.result)
                elif hasattr(event, 'error'):
                    await observer.on_error(event.error)
            except Exception as e:
                logger.error(f"Observer notification failed: {e}")
                
    async def start(self) -> None:
        """스케줄러 시작"""
        if self._status == SchedulerStatus.RUNNING:
            logger.warning(f"Scheduler {self.scheduler_id} is already running")
            return
            
        logger.info(f"Starting scheduler {self.scheduler_id}")
        self._status = SchedulerStatus.RUNNING
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        
    async def stop(self) -> None:
        """스케줄러 정지"""
        if self._status == SchedulerStatus.STOPPED:
            return
            
        logger.info(f"Stopping scheduler {self.scheduler_id}")
        self._status = SchedulerStatus.STOPPED
        self._shutdown_event.set()
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
            
    async def pause(self) -> None:
        """스케줄러 일시 정지"""
        if self._status == SchedulerStatus.RUNNING:
            self._status = SchedulerStatus.PAUSED
            logger.info(f"Scheduler {self.scheduler_id} paused")
            
    async def resume(self) -> None:
        """스케줄러 재개"""
        if self._status == SchedulerStatus.PAUSED:
            self._status = SchedulerStatus.RUNNING
            logger.info(f"Scheduler {self.scheduler_id} resumed")
            
    def get_status(self) -> SchedulerStatus:
        """현재 상태 반환"""
        return self._status
        
    async def _run_loop(self) -> None:
        """메인 실행 루프"""
        # 첫 번째 실행을 즉시 수행
        first_run = True
        
        while not self._shutdown_event.is_set():
            if self._status == SchedulerStatus.RUNNING:
                try:
                    await self._execute_cycle()
                except Exception as e:
                    logger.error(f"Scheduler cycle failed: {e}")
                    await self._handle_error(e)
            
            # 첫 번째 실행 후부터 간격 대기
            if first_run:
                first_run = False
                logger.info(f"Scheduler {self.scheduler_id} completed first run, waiting {self.interval_seconds}s for next cycle")
                    
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), 
                    timeout=self.interval_seconds
                )
                break  # shutdown_event가 설정됨
            except asyncio.TimeoutError:
                continue  # 다음 사이클 실행
                
    async def _execute_cycle(self) -> None:
        """한 사이클 실행"""
        cycle_start = datetime.now()
        logger.info(f"Starting collection cycle for {self.scheduler_id}")
        
        # 1. 메트릭 스냅샷 생성 (기존 trend_batch 기능 통합)
        await self._create_metrics_snapshot()
        
        # 2. 영상 수집 단계
        await self._notify_observers(CollectionStartedEvent(
            scheduler_id=self.scheduler_id,
            timestamp=cycle_start,
            target_count=50,  # 기본값
            metadata={}
        ))
        
        collection_result = await self.collector.collect_videos()
        
        await self._notify_observers(CollectionCompletedEvent(
            scheduler_id=self.scheduler_id,
            timestamp=datetime.now(),
            result=collection_result,
            metadata={}
        ))
        
        # 3. 트렌드 분석 단계
        if collection_result.videos:
            await self._notify_observers(AnalysisStartedEvent(
                scheduler_id=self.scheduler_id,
                timestamp=datetime.now(),
                video_count=len(collection_result.videos),
                metadata={}
            ))
            
            analysis_result = await self.analyzer.analyze_trends(collection_result.videos)
            
            await self._notify_observers(AnalysisCompletedEvent(
                scheduler_id=self.scheduler_id,
                timestamp=datetime.now(),
                result=analysis_result,
                metadata={}
            ))
            
        # 4. 트렌드 집계 실행 (기존 trend_batch 기능 통합)
        await self._run_trend_aggregation()
        
        # 5. 카테고리 태그 집계 (기존 youtube_tag_batch 기능 통합)
        await self._update_category_tags()
            
        cycle_end = datetime.now()
        execution_time = (cycle_end - cycle_start).total_seconds()
        logger.info(f"Collection cycle completed in {execution_time:.2f}s")
        
    async def _handle_error(self, error: Exception) -> None:
        """에러 처리"""
        self._status = SchedulerStatus.ERROR
        
        error_event = ErrorEvent(
            scheduler_id=self.scheduler_id,
            timestamp=datetime.now(),
            error=error,
            error_context=f"Scheduler {self.scheduler_id} execution",
            metadata={}
        )
        
        await self._notify_observers(error_event)
        
        # 에러 후 일정 시간 대기 후 재시작
        await asyncio.sleep(60)  # 1분 대기
        if not self._shutdown_event.is_set():
            self._status = SchedulerStatus.RUNNING
            
    async def _create_metrics_snapshot(self) -> None:
        """영상 메트릭 스냅샷 생성 (기존 trend_batch 기능)"""
        try:
            from datetime import date
            from sqlalchemy import text
            from config.database.session import SessionLocal
            
            with SessionLocal() as db:
                db.execute(
                    text("""
                        INSERT INTO video_metrics_snapshot (video_id, platform, snapshot_date, view_count, like_count, comment_count)
                        SELECT
                            v.video_id,
                            v.platform,
                            :snapshot_date,
                            v.view_count,
                            v.like_count,
                            v.comment_count
                        FROM video v
                        WHERE v.platform = :platform
                        ON CONFLICT (video_id, snapshot_date, platform)
                        DO UPDATE SET
                            view_count = EXCLUDED.view_count,
                            like_count = EXCLUDED.like_count,
                            comment_count = EXCLUDED.comment_count
                    """),
                    {"snapshot_date": date.today(), "platform": "youtube"},
                )
                db.commit()
                logger.info("Metrics snapshot created successfully")
                
        except Exception as e:
            logger.error(f"Failed to create metrics snapshot: {e}")
            
    async def _run_trend_aggregation(self) -> None:
        """트렌드 집계 실행 (기존 trend_batch 기능)"""
        try:
            from content.application.usecase.trend_aggregation_usecase import TrendAggregationUseCase
            from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
            from datetime import date
            
            usecase = TrendAggregationUseCase(ContentRepositoryImpl())
            result = usecase.aggregate(as_of=date.today(), window_days=7, platform="youtube")
            logger.info(f"Trend aggregation completed: {result}")
            
        except Exception as e:
            logger.error(f"Failed to run trend aggregation: {e}")
            
    async def _update_category_tags(self) -> None:
        """카테고리별 태그 집계 업데이트 (기존 youtube_tag_batch 기능)"""
        try:
            from sqlalchemy import text
            from config.database.session import SessionLocal
            
            with SessionLocal() as db:
                # category_trend에서 카테고리 목록 조회
                category_rows = db.execute(
                    text("""
                        SELECT DISTINCT category
                        FROM category_trend
                        WHERE platform = :platform
                          AND category IS NOT NULL
                    """),
                    {"platform": "youtube"},
                ).mappings().all()
                
                for row in category_rows:
                    category = row["category"]
                    await self._insert_category_trend_tags(category, db)
                    
                logger.info(f"Updated tags for {len(category_rows)} categories")
                
        except Exception as e:
            logger.error(f"Failed to update category tags: {e}")
            
    async def _insert_category_trend_tags(self, category: str, db) -> None:
        """특정 카테고리의 트렌드 태그 삽입"""
        try:
            from sqlalchemy import text
            
            # 카테고리별 상위 5개 태그 집계
            tags_row = db.execute(
                text("""
                    WITH splitted AS (
                        SELECT trim(tag) AS tag
                        FROM video v
                        JOIN video_sentiment vs ON vs.video_id = v.video_id
                        CROSS JOIN LATERAL unnest(string_to_array(COALESCE(v.tags, ''), ',')) AS tag
                        WHERE vs.category = :category
                          AND COALESCE(v.tags, '') <> ''
                    ),
                    ranked AS (
                        SELECT tag, COUNT(*) AS cnt
                        FROM splitted
                        GROUP BY tag
                        ORDER BY cnt DESC
                        LIMIT 5
                    )
                    SELECT string_agg(tag, ',' ORDER BY cnt DESC) AS tags
                    FROM ranked
                """),
                {"category": category},
            ).mappings().one_or_none()

            tags_value = (tags_row or {}).get("tags") or ""

            # 기존 데이터 삭제 후 새로 삽입
            db.execute(
                text("DELETE FROM category_trend_tag WHERE category = :category"),
                {"category": category},
            )

            db.execute(
                text("""
                    INSERT INTO category_trend_tag (category, tags, create_at)
                    VALUES (:category, :tags, NOW())
                """),
                {"category": category, "tags": tags_value},
            )
            db.commit()
            
        except Exception as e:
            logger.error(f"Failed to insert category trend tags for {category}: {e}")