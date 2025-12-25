# middleware.py
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import json
from content.infrastructure.repository.stopword_repository_impl import StopwordRepositoryImpl
from content.application.usecase.stopword_usecase import StopwordUseCase

class StopwordMiddleware(BaseHTTPMiddleware):

    __instance = None
    _usecase = None

    def __new__(cls, app, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            repo = StopwordRepositoryImpl.getInstance()
            if repo is None:
               raise RuntimeError("StopwordRepositoryImpl.getInstance() returned None")
            cls._usecase = StopwordUseCase(repo, lang="ko")
        return cls.__instance
    
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        if request.method in ("POST", "PUT"):
            # Request를 새로 만들어 body 수정
            body = await request.body()
            try:
                json_body = json.loads(body)
                #print(f"json_body={json_body}")
                
                # 텍스트 필드만 불용어 제거 (예: text, content, comment 등)
                for key, value in json_body.items():
                    if isinstance(value, str):
                        json_body[key] = self._usecase.remove_stopwords(value)
                modified_body = json.dumps(json_body).encode('utf-8')
                request._body = modified_body

            except json.JSONDecodeError:
                pass  # JSON이 아니면 그대로 진행
            
        return await call_next(request)
