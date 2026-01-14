from abc import ABC, abstractmethod

class VideoStoragePort(ABC):
    @abstractmethod
    def download(self, url: str) -> str:
        """URL로부터 영상을 다운로드하고 로컬 경로를 반환"""
        pass

    @abstractmethod
    def delete(self, local_path: str):
        """작업 완료 후 로컬 파일 삭제"""
        pass