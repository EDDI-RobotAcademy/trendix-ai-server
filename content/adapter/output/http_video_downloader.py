import aiohttp
import aiofiles
import os
import asyncio
import uuid
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional
import yt_dlp

from content.application.port.video_downloader_port import VideoDownloader


class HTTPVideoDownloader(VideoDownloader):
    def __init__(
            self,
            temp_dir: str = "/tmp/videos",
            max_file_size_mb: int = 500,
            timeout_seconds: int = 600,
            allowed_domains: Optional[list] = None
    ):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.timeout = timeout_seconds
        self.allowed_domains = allowed_domains or []

    def _validate_url(self, url: str) -> None:
        """URL 보안 검증"""
        parsed = urlparse(url)

        # 스키마 검증
        if parsed.scheme not in ['http', 'https']:
            raise ValueError(f"Invalid URL scheme: {parsed.scheme}")

        # 도메인 화이트리스트 검증 (설정된 경우에만)
        if self.allowed_domains:
            if not any(domain in parsed.netloc for domain in self.allowed_domains):
                raise ValueError(f"Domain {parsed.netloc} not in allowed list")

        # 로컬 네트워크 차단 (SSRF 방지)
        if parsed.netloc in ['localhost', '127.0.0.1', '0.0.0.0']:
            raise ValueError("Local network access not allowed")

    def _is_youtube_url(self, url: str) -> bool:
        """유튜브 URL인지 확인"""
        youtube_domains = ['youtube.com', 'youtu.be', 'www.youtube.com']
        parsed = urlparse(url)
        return any(domain in parsed.netloc for domain in youtube_domains)

    async def download(self, video_url: str) -> str:
        """비동기 영상 다운로드"""
        # URL 검증
        self._validate_url(video_url)

        # 고유 파일명 생성
        file_name = f"{uuid.uuid4().hex}.mp4"
        file_path = self.temp_dir / file_name

        try:
            if self._is_youtube_url(video_url):
                await self._download_youtube(video_url, file_path)
            else:
                await self._download_http(video_url, file_path)

            # 파일 크기 검증
            file_size = os.path.getsize(file_path)
            if file_size > self.max_file_size:
                raise ValueError(f"File size {file_size} exceeds limit {self.max_file_size}")

            return str(file_path)

        except Exception as e:
            # 실패 시 임시 파일 정리
            if file_path.exists():
                file_path.unlink()
            raise Exception(f"Video download failed: {str(e)}") from e

    async def _download_youtube(self, video_url: str, file_path: Path) -> None:
        """yt-dlp를 사용한 유튜브 동영상 다운로드"""
        ydl_opts = {
            'format': 'bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'outtmpl': str(file_path),
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,  # 플레이리스트 다운로드 방지
            'socket_timeout': self.timeout,
            'max_filesize': self.max_file_size,
        }

        # yt-dlp는 동기 함수이므로 별도 스레드에서 실행
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._run_ytdlp,
            video_url,
            ydl_opts
        )

    def _run_ytdlp(self, url: str, opts: dict) -> None:
        """yt-dlp 실행 (스레드에서 실행됨)"""
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])

    async def _download_http(self, video_url: str, file_path: Path) -> None:
        """일반 HTTP URL에서 동영상 다운로드"""
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(video_url) as response:
                response.raise_for_status()

                # Content-Length 확인
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > self.max_file_size:
                    raise ValueError(f"File size {content_length} exceeds limit")

                # 청크 단위로 다운로드 (1MB 청크)
                downloaded_size = 0
                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        downloaded_size += len(chunk)

                        # 다운로드 중 크기 체크
                        if downloaded_size > self.max_file_size:
                            raise ValueError(f"Download exceeds size limit")

                        await f.write(chunk)

    async def cleanup(self, file_path: str) -> None:
        """비동기 파일 삭제"""
        try:
            path = Path(file_path)
            if path.exists():
                # 비동기 파일 삭제
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, path.unlink)
        except Exception as e:
            print(f"Failed to cleanup {file_path}: {e}")