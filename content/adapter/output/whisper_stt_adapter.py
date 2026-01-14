from typing import Dict
import gc
import torch
import whisper

from content.application.port.stt_service_port import STTServicePort


class WhisperSTTAdapter(STTServicePort):
    def __init__(self, model_name: str = "base", use_gpu: bool = True):
        # GPU 사용 가능 여부 확인 및 디바이스 설정
        self.device = "cuda" if use_gpu and torch.cuda.is_available() else "cpu"
        self.model = whisper.load_model(model_name, device=self.device)
        print(f"Whisper STT 모델이 {self.device} 디바이스에서 로드되었습니다.")

    async def transcribe(self, video_path: str) -> Dict:
        try:
            # 작업 시작 시 GPU로 이동
            if self.device == "cuda":
                self.model.to(self.device)
                
            result = self.model.transcribe(video_path, language="ko")
            
            return {
                'text': result['text'],
                'segments': result['segments']
            }
        finally:
            # GPU 메모리 정리 및 CPU로 오프로드
            if self.device == "cuda":
                self.model.to("cpu")
                gc.collect()
                torch.cuda.empty_cache()