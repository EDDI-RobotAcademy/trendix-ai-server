import whisper
from ultralytics import YOLO

from content.application.port.ai_analysis_port import AiAnalysisPort


class AiModelClient(AiAnalysisPort):  # 인터페이스 상속(구현)
    def __init__(self):
        # 실제 무거운 모델 로드
        self.stt_model = whisper.load_model("base")
        self.yolo_model = YOLO("yolov8n.pt")

    def transcribe_audio(self, video_path):
        # 실제 Whisper 동작 로직
        return self.stt_model.transcribe(video_path)

    def analyze_visuals(self, video_path):
        # 실제 YOLO 동작 로직
        return self.yolo_model(video_path)

    def run_full_analysis(self, video_path):
        # 두 기능을 합쳐서 결과 반환
        return {
            "transcript": self.transcribe_audio(video_path),
            "visuals": self.analyze_visuals(video_path)
        }