from typing import List
import gc
import torch
from ultralytics import YOLO

from content.application.port.object_detection_port import ObjectDetectionPort
from content.domain.video_analysis import VisualFrame, DetectedObject


class YOLODetectionAdapter(ObjectDetectionPort):
    def __init__(self, model_path: str = 'yolov8n.pt', sample_interval: int = 2, use_gpu: bool = True):
        self.device = "cuda" if use_gpu and torch.cuda.is_available() else "cpu"
        self.model = YOLO(model_path)
        self.model.to(self.device)
        self.sample_interval = sample_interval
        
        if self.device == "cuda":
            print(f"GPU: {torch.cuda.get_device_name(0)}")

    async def detect_objects(self, video_path: str) -> List[VisualFrame]:
        import cv2
        
        # FPS 정보만 가져오기
        cap = cv2.VideoCapture(video_path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.release()
        
        frame_interval = int(fps * self.sample_interval)
        print(f"영상: {fps} FPS, {total_frames} 프레임, {frame_interval}프레임마다 샘플링")
        
        frames = []
        frame_count = 0
        processed = 0
        
        try:
            # YOLO가 직접 비디오 처리 (훨씬 빠름!)
            results_generator = self.model.predict(
                source=video_path,
                stream=True,           # 스트리밍 모드
                device=self.device,
                verbose=False,
                vid_stride=frame_interval  # 핵심! N프레임마다 처리
            )
            
            for results in results_generator:
                timestamp = frame_count * frame_interval / fps
                
                objects = []
                boxes = results.boxes
                for cls, conf in zip(boxes.cls, boxes.conf):
                    objects.append(DetectedObject(
                        class_name=results.names[int(cls)],
                        confidence=float(conf)
                    ))
                
                frames.append(VisualFrame(
                    timestamp=timestamp,
                    objects=objects
                ))
                
                frame_count += 1
                processed += 1
                
                if processed % 50 == 0:
                    progress = (frame_count * frame_interval / total_frames) * 100
                    print(f"{processed} 프레임 분석 완료 ({progress:.1f}%)")
            
            print(f"완료: {processed} 프레임 분석")
            return frames
            
        finally:
            if self.device == "cuda":
                gc.collect()
                torch.cuda.empty_cache()