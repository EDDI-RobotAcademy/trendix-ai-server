from typing import List
import numpy as np
from collections import defaultdict

from content.application.port.embedding_generator_port import EmbeddingGeneratorPort
from content.application.port.embedding_repository_port import EmbeddingRepositoryPort
from content.application.port.video_repository_port import VideoRepositoryPort
from content.domain.embedding import EmbeddingData, ChunkData


class EmbeddingService:
    def __init__(
            self,
            embedding_generator: EmbeddingGeneratorPort,
            embedding_repository: EmbeddingRepositoryPort,
            video_repository: VideoRepositoryPort,
            target_chunk_duration: float = 7.0,
            scene_change_threshold: float = 0.3
    ):
        self.embedding_generator = embedding_generator
        self.embedding_repository = embedding_repository
        self.video_repository = video_repository
        self.target_chunk_duration = target_chunk_duration
        self.scene_change_threshold = scene_change_threshold

    async def generate_embeddings(self, video_id: str) -> None:
        try:
            # 1. 분석 결과 가져오기
            analysis = await self.video_repository.get_analysis(video_id)

            print(f"분석 데이터 로드 완료: {video_id}")
            if not analysis:
                raise ValueError(f"No analysis found for video_id: {video_id}")

            # 2. 개선된 청크 생성
            chunks = self._create_improved_chunks(analysis)
            
            print(f"청크 생성 완료: {len(chunks)}개")
            
            if not chunks:
                print("생성된 chunk가 없습니다.")
                return
            
            # 3. 임베딩 생성
            texts = [chunk.text for chunk in chunks]
            embeddings = await self.embedding_generator.generate_batch_embeddings(texts)

            print("임베딩 생성 완료")
            
            # 4. 저장
            embedding_data_list = [
                EmbeddingData(
                    video_id=video_id,
                    chunk_type=chunk.chunk_type,
                    chunk_text=chunk.text,
                    chunk_metadata=chunk.metadata,
                    embedding=embedding
                )
                for chunk, embedding in zip(chunks, embeddings)
            ]

            await self.embedding_repository.save_embeddings(embedding_data_list)
            print(f"임베딩 저장 완료: {len(embedding_data_list)}개")
            
        except Exception as e:
            print(f"임베딩 생성 중 오류 발생: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def _create_improved_chunks(self, analysis) -> List[ChunkData]:
        """개선된 chunking 전략"""
        chunks = []

        try:
            # 1. Transcript: 시간 기반 (5-10초 단위)
            if analysis.transcript_segments:
                transcript_chunks = self._chunk_transcript_by_time(analysis)
                chunks.extend(transcript_chunks)
                print(f"Transcript chunks 생성: {len(transcript_chunks)}개")

            # 2. Visual: Scene change 감지 기준
            if analysis.visual_frames:
                visual_chunks = self._chunk_visual_by_scene(analysis)
                chunks.extend(visual_chunks)
                print(f"Visual chunks 생성: {len(visual_chunks)}개")

        except Exception as e:
            print(f"Chunk 생성 중 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

        return chunks

    def _chunk_transcript_by_time(self, analysis) -> List[ChunkData]:
        """시간 기반 transcript chunking (5-10초 단위)"""
        chunks = []
        
        if not analysis.transcript_segments:
            return chunks

        current_chunk = []
        current_start = analysis.transcript_segments[0].start
        current_duration = 0.0
        
        for idx, segment in enumerate(analysis.transcript_segments):
            segment_duration = segment.end - segment.start
            
            # 현재 chunk에 추가
            current_chunk.append(segment)
            current_duration += segment_duration
            
            # 10초 이상이면 chunk 생성
            if current_duration >= self.target_chunk_duration:
                self._save_transcript_chunk(
                    chunks, current_chunk, current_start, current_duration
                )
                
                # 다음 chunk 준비
                if idx < len(analysis.transcript_segments) - 1:
                    next_segment = analysis.transcript_segments[idx + 1]
                    current_start = next_segment.start
                    current_chunk = []
                    current_duration = 0.0
        
        # 마지막 남은 chunk 처리
        if current_chunk:
            self._save_transcript_chunk(
                chunks, current_chunk, current_start, current_duration
            )
        
        return chunks

    def _save_transcript_chunk(
        self, 
        chunks: List[ChunkData], 
        current_chunk: List, 
        current_start: float, 
        current_duration: float
    ):
        """Transcript chunk 저장 헬퍼"""
        chunk_text = ' '.join([seg.text for seg in current_chunk])
        chunk_end = current_chunk[-1].end
        
        chunks.append(ChunkData(
            chunk_type='transcript',
            text=chunk_text,
            metadata={
                'start_time': current_start,
                'end_time': chunk_end,
                'duration': current_duration,
                'sentence_count': len(current_chunk),
                'word_count': len(chunk_text.split()),
                'timestamps': [
                    {'start': seg.start, 'end': seg.end, 'text': seg.text}
                    for seg in current_chunk
                ]
            }
        ))

    def _chunk_visual_by_scene(self, analysis) -> List[ChunkData]:
        """Scene change 기반 visual chunking"""
        chunks = []
        
        if not analysis.visual_frames or len(analysis.visual_frames) < 2:
            return chunks

        try:
            # Scene change 감지
            scenes = self._detect_scene_changes(analysis.visual_frames)
            
            for scene_id, scene in enumerate(scenes):
                # Scene 설명 생성
                scene_description = self._generate_scene_description(scene)
                
                chunks.append(ChunkData(
                    chunk_type='visual',
                    text=scene_description,
                    metadata={
                        'start_time': scene['start'],
                        'end_time': scene['end'],
                        'duration': scene['end'] - scene['start'],
                        'scene_id': scene_id,
                        'frame_count': len(scene['frames']),
                        'dominant_objects': scene['dominant_objects'],
                        'scene_change_score': scene.get('change_score', 0.0),
                        'key_frames': scene['key_frame_timestamps']
                    }
                ))
        
        except Exception as e:
            print(f"Visual chunk 생성 중 오류: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return chunks

    def _detect_scene_changes(self, visual_frames: List) -> List[dict]:
        """연속 프레임 간 유사도로 scene change 감지"""
        
        if not visual_frames:
            return []
        
        scenes = []
        
        # 첫 프레임으로 첫 scene 시작
        first_frame = visual_frames[0]
        current_scene = {
            'start': first_frame.timestamp,  # 속성 접근
            'frames': [first_frame],
            'key_frames': [first_frame]
        }
        
        for i in range(1, len(visual_frames)):
            prev_frame = visual_frames[i-1]
            curr_frame = visual_frames[i]
            
            # 프레임 간 유사도 계산
            similarity = self._calculate_frame_similarity(prev_frame, curr_frame)
            
            # Scene change 감지
            if similarity < self.scene_change_threshold:
                # 현재 scene 종료
                current_scene['end'] = prev_frame.timestamp  # 속성 접근
                current_scene['change_score'] = 1.0 - similarity
                
                # Scene 정보 집계
                self._finalize_scene(current_scene)
                
                scenes.append(current_scene)
                
                # 새 scene 시작
                current_scene = {
                    'start': curr_frame.timestamp,  # 속성 접근
                    'frames': [curr_frame],
                    'key_frames': [curr_frame]
                }
            else:
                current_scene['frames'].append(curr_frame)
        
        # 마지막 scene 처리
        if current_scene['frames']:
            last_frame = visual_frames[-1]
            current_scene['end'] = last_frame.timestamp  # 속성 접근
            current_scene['change_score'] = 0.0
            
            self._finalize_scene(current_scene)
            
            scenes.append(current_scene)
        
        return scenes

    def _finalize_scene(self, scene: dict):
        """Scene 정보 마무리 (공통 로직)"""
        # Dominant objects 집계
        scene['dominant_objects'] = self._aggregate_objects(scene['frames'])
        
        # Key frames 선택 (균등 분포)
        frame_count = len(scene['frames'])
        if frame_count > 3:
            step = frame_count // 3
            scene['key_frames'] = [
                scene['frames'][0],
                scene['frames'][step],
                scene['frames'][step * 2]
            ]
        
        # Key frame 타임스탬프만 추출
        scene['key_frame_timestamps'] = [
            frame.timestamp for frame in scene['key_frames']  # 속성 접근
        ]

    def _calculate_frame_similarity(self, frame1, frame2) -> float:
        """
        두 프레임 간 유사도 계산 (객체 기반)
        """
        # 속성으로 접근 (frame.objects)
        objects1 = set([obj.class_name for obj in frame1.objects])
        objects2 = set([obj.class_name for obj in frame2.objects])
        
        if not objects1 and not objects2:
            return 1.0
        
        if not objects1 or not objects2:
            return 0.0
        
        # Jaccard 유사도
        intersection = len(objects1 & objects2)
        union = len(objects1 | objects2)
        
        return intersection / union if union > 0 else 0.0

    def _aggregate_objects(self, frames: List) -> List[dict]:
        """프레임들의 객체를 집계하여 대표 객체 추출"""
        object_counts = defaultdict(lambda: {'count': 0, 'total_confidence': 0.0})
        
        for frame in frames:
            # 속성으로 접근 (frame.objects)
            for obj in frame.objects:
                object_counts[obj.class_name]['count'] += 1
                object_counts[obj.class_name]['total_confidence'] += obj.confidence
        
        # 평균 confidence 계산 및 정렬
        aggregated = [
            {
                'class_name': name,
                'frequency': data['count'] / len(frames),
                'avg_confidence': data['total_confidence'] / data['count']
            }
            for name, data in object_counts.items()
        ]
        
        # Frequency * Confidence로 정렬
        aggregated.sort(
            key=lambda x: x['frequency'] * x['avg_confidence'],
            reverse=True
        )
        
        return aggregated[:5]  # 상위 5개만

    def _generate_scene_description(self, scene: dict) -> str:
        """Scene을 자연어로 설명"""
        
        duration = scene['end'] - scene['start']
        dominant_objects = scene['dominant_objects']
        
        if not dominant_objects:
            return (
                f"Scene from {scene['start']:.1f}s to {scene['end']:.1f}s "
                f"({duration:.1f}s duration)"
            )
        
        # 주요 객체들 설명
        top_objects = [
            f"{obj['class_name']} (seen in {obj['frequency']*100:.0f}% of frames)"
            for obj in dominant_objects[:3]
        ]
        
        objects_desc = ", ".join(top_objects)
        
        # 장면 타입 추론
        scene_type = self._infer_scene_type(dominant_objects)
        
        description = (
            f"Scene: {scene_type} setting from {scene['start']:.1f}s to {scene['end']:.1f}s. "
            f"Duration: {duration:.1f}s. "
            f"Main elements: {objects_desc}."
        )
        
        return description

    def _infer_scene_type(self, objects: List[dict]) -> str:
        """객체 정보로 장면 타입 추론"""
        
        object_names = [obj['class_name'].lower() for obj in objects]
        
        # 간단한 규칙 기반 분류
        if any(obj in object_names for obj in ['person', 'chair', 'dining table']):
            return 'indoor conversation'
        elif any(obj in object_names for obj in ['car', 'traffic light', 'bicycle']):
            return 'outdoor/street'
        elif any(obj in object_names for obj in ['laptop', 'keyboard', 'monitor']):
            return 'workspace'
        elif any(obj in object_names for obj in ['bottle', 'cup', 'bowl']):
            return 'dining/kitchen'
        else:
            return 'general'