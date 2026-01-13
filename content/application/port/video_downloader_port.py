from abc import ABC, abstractmethod


class VideoDownloader(ABC):
    @abstractmethod
    async def download(self, video_url: str) -> str:
        """영상을 다운로드하고 로컬 경로 반환"""
        pass

    @abstractmethod
    async def cleanup(self, file_path: str) -> None:
        """임시 파일 삭제"""
        pass