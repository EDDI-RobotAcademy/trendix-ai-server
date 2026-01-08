# middleware.py
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.datastructures import FormData
from starlette.responses import JSONResponse
from typing import Dict, Any
import json
from content.infrastructure.repository.stopword_repository_impl import StopwordRepositoryImpl
from content.application.usecase.stopword_usecase import StopwordUseCase

class StopwordMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):

        repo = StopwordRepositoryImpl()  # getInstance() 제거
        usecase = StopwordUseCase(repo, lang="ko")

        try:
            if request.method in ("POST", "PUT"):
                content_type = request.headers.get("content-type", "")

                # 이미 body 를 한 번 읽으면 사라지기 때문에, 먼저 raw body 확보
                body_bytes = await request.body()
                
                # 1) JSON 처리
                if "application/json" in content_type:
                    try:
                        # Request를 새로 만들어 body 수정
                        body = body_bytes.decode("utf-8")
                        json_body = json.loads(body)
                        cleaned_body = usecase.remove_stopwords_iterative(json_body)
                        request.state.cleaned_body = cleaned_body

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        print(f"JSON/UTF-8 decode error: {e}")
                        # JSON이 아니거나 UTF-8 디코딩 실패 시 그대로 진행

                # 2) form-urlencoded / multipart 처리
                elif (
                    "application/x-www-form-urlencoded" in content_type
                    or "multipart/form-data" in content_type
                ):

                    # body_bytes 를 다시 request 에 세팅해서 request.form() 이 동작할 수 있게 함
                    async def _reset_receive():
                        return {"type": "http.request", "body": body_bytes, "more_body": False}
                    request._receive = _reset_receive

                    form: FormData = await request.form()
                    new_form: Dict[str, Any] = {}

                    for key, value in form.multi_items():
                        # 파일은 그대로 두고, 문자열만 불용어 제거
                        if isinstance(value, str):
                            new_form[key] = usecase.remove_stopwords(value)
                        else:
                            new_form[key] = value
                    
                    request.state.cleaned_form = new_form

                else:
                    # 기타 content-type 은 건드리지 않음
                    pass

            # 미들웨어 최종 반환값 강제
            response = await call_next(request)
            if response is None:
                return JSONResponse({"error": "Internal Server Error"}, status_code=500)
            return response
        
        except Exception as e:
            print(f"Middleware error: {e}")
            # 예외 발생 시 원본 request 그대로 넘김
            response = await call_next(request)
            if response is None:
                return JSONResponse({"error": "Internal Server Error"}, status_code=500)
            return response

