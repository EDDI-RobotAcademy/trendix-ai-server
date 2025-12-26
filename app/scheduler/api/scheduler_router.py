from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from ..youtube_trend_scheduler import get_scheduler_service
from ..core.interfaces import SchedulerStatus

router = APIRouter()


@router.get("/schedulers/status")
async def get_all_scheduler_status() -> Dict[str, Any]:
    """모든 스케줄러의 상태 조회"""
    try:
        service = get_scheduler_service()
        manager = service.get_manager()
        statuses = manager.get_all_statuses()
        
        return {
            "schedulers": {
                scheduler_id: status.value
                for scheduler_id, status in statuses.items()
            },
            "total_count": len(statuses),
            "running_count": sum(1 for status in statuses.values() if status == SchedulerStatus.RUNNING),
            "stopped_count": sum(1 for status in statuses.values() if status == SchedulerStatus.STOPPED),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {str(e)}")


@router.get("/schedulers/{scheduler_id}/status")
async def get_scheduler_status(scheduler_id: str) -> Dict[str, Any]:
    """특정 스케줄러의 상태 조회"""
    try:
        service = get_scheduler_service()
        manager = service.get_manager()
        status = manager.get_scheduler_status(scheduler_id)
        
        if status is None:
            raise HTTPException(status_code=404, detail=f"Scheduler {scheduler_id} not found")
            
        return {
            "scheduler_id": scheduler_id,
            "status": status.value
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {str(e)}")


@router.post("/schedulers/{scheduler_id}/start")
async def start_scheduler(scheduler_id: str) -> Dict[str, Any]:
    """특정 스케줄러 시작"""
    try:
        service = get_scheduler_service()
        manager = service.get_manager()
        
        await manager.start_scheduler(scheduler_id)
        
        return {
            "scheduler_id": scheduler_id,
            "action": "started",
            "status": "success"
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {str(e)}")


@router.post("/schedulers/{scheduler_id}/stop")
async def stop_scheduler(scheduler_id: str) -> Dict[str, Any]:
    """특정 스케줄러 정지"""
    try:
        service = get_scheduler_service()
        manager = service.get_manager()
        
        await manager.stop_scheduler(scheduler_id)
        
        return {
            "scheduler_id": scheduler_id,
            "action": "stopped", 
            "status": "success"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {str(e)}")


@router.get("/schedulers")
async def list_schedulers() -> Dict[str, Any]:
    """등록된 모든 스케줄러 목록 조회"""
    try:
        service = get_scheduler_service()
        manager = service.get_manager()
        scheduler_ids = manager.list_schedulers()
        
        return {
            "schedulers": scheduler_ids,
            "total_count": len(scheduler_ids)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list schedulers: {str(e)}")