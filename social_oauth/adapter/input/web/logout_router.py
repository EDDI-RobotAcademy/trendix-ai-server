import os

from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse, JSONResponse

from config.redis_config import get_redis

logout_router = APIRouter()

redis_client = get_redis()

@logout_router.get("/")
async def logout(request: Request):
    """Logout user by deleting session from Redis and clearing cookie."""
    session_id = request.cookies.get("session_id")
    if session_id:
        # Remove session data from Redis if it exists
        redis_client.delete(session_id)
    # Redirect to front‑end (or home) after logout
    response = JSONResponse(
        content={"message": "Logged out successfully"},
        status_code=200
    )
    response.delete_cookie(key="session_id")
    return response