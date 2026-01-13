import os
import requests
from urllib.parse import quote

from social_oauth.adapter.input.web.request.get_access_token_request import GetAccessTokenRequest
from social_oauth.adapter.input.web.response.access_token import AccessToken

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"

class GoogleOAuth2Service:

    def get_authorization_url(self) -> str:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI")  # quote() 제거: 토큰 요청과 일치시킴
        scope = "openid email profile"
        return (
            f"{GOOGLE_AUTH_URL}"
            f"?client_id={client_id}"
            f"&redirect_uri={redirect_uri}"
            f"&response_type=code"
            f"&scope={quote(scope)}"
        )

    def refresh_access_token(self, request: GetAccessTokenRequest) -> AccessToken:
        data = {
            "code": request.code,
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
            "redirect_uri": os.getenv("GOOGLE_REDIRECT_URI"),
            "grant_type": "authorization_code"
        }
        
        # 디버깅: 토큰 요청 정보 출력
        print(f"[DEBUG] Token Request to {GOOGLE_TOKEN_URL}")
        print(f"[DEBUG] redirect_uri: {data['redirect_uri']}")
        print(f"[DEBUG] client_id: {data['client_id']}")
        print(f"[DEBUG] code: {data['code'][:30]}..." if len(data['code']) > 30 else f"[DEBUG] code: {data['code']}")
        
        resp = requests.post(GOOGLE_TOKEN_URL, data=data)
        
        # 에러 시 상세 정보 출력
        if not resp.ok:
            print(f"[ERROR] Google Token API returned {resp.status_code}")
            print(f"[ERROR] Response: {resp.text}")
        
        resp.raise_for_status()
        token_data = resp.json()
        # Pydantic 모델에 맞춰서 변환
        return AccessToken(
            access_token=token_data.get("access_token"),
            token_type=token_data.get("token_type"),
            expires_in=token_data.get("expires_in"),
            refresh_token=token_data.get("refresh_token")
        )

    def fetch_user_profile(self, access_token: AccessToken) -> dict:
        headers = {"Authorization": f"Bearer {access_token.access_token}"}
        resp = requests.get(GOOGLE_USERINFO_URL, headers=headers)
        resp.raise_for_status()
        return resp.json()
