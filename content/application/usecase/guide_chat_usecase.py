from __future__ import annotations

import json
import logging
import re
from typing import List, Tuple, Optional

from openai import OpenAI, Stream
from openai.types.chat import ChatCompletionChunk

from config.settings import OpenAISettings
from content.application.port.embedding_generator_port import EmbeddingGeneratorPort
from content.application.port.embedding_repository_port import EmbeddingRepositoryPort
from content.application.port.video_repository_port import VideoRepositoryPort
from content.domain.video_analysis import VideoAnalysisResult

# 로거 설정
logger = logging.getLogger(__name__)

class GuideChatUseCase:
    """
    영상 분석 데이터를 기반으로 사용자에게 가이드를 제공하는 유스케이스.
    - 질문과 유사한 영상 내 구간(텍스트/시각 정보)을 검색하여 GPT 컨텍스트로 제공.
    """

    def __init__(
        self,
        embedding_generator: EmbeddingGeneratorPort,
        embedding_repository: EmbeddingRepositoryPort,
        video_repository: VideoRepositoryPort,
        settings: OpenAISettings | None = None,
    ):
        self.embedding_generator = embedding_generator
        self.embedding_repository = embedding_repository
        self.video_repository = video_repository
        self.settings = settings or OpenAISettings()
        if not self.settings.api_key:
            raise ValueError("OPENAI_API_KEY is required for GuideChatUseCase")
        self.client = OpenAI(api_key=self.settings.api_key)

    def _parse_duration_to_seconds(self, duration: Optional[str]) -> Optional[int]:
        """ISO 8601 duration (PT1M30S) 또는 초 단위 문자열을 초 단위 정수로 변환"""
        if not duration:
            return None
        
        # 이미 숫자인 경우
        if duration.isdigit():
            return int(duration)
        
        # ISO 8601 형식 (PT1H2M30S)
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if match:
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0)
            seconds = int(match.group(3) or 0)
            return hours * 3600 + minutes * 60 + seconds
        
        return None

    def _classify_video_type(self, duration_seconds: Optional[int]) -> str:
        """영상 길이에 따라 유형 분류"""
        if duration_seconds is None:
            return "영상"
        elif duration_seconds <= 60:
            return "숏츠(60초 이하 짧은 영상)"
        elif duration_seconds <= 180:
            return "짧은 영상(1~3분)"
        elif duration_seconds <= 600:
            return "중간 길이 영상(3~10분)"
        else:
            return "긴 영상(10분 이상)"

    async def answer_with_guide(
        self,
        user_messages: List[dict],
        video_id: Optional[str] = None,
        limit: int = 20
    ) -> Stream[ChatCompletionChunk]:
        # 1. 유저 질문 추출
        query = ""
        for msg in reversed(user_messages):
            if msg.get("role") == "user":
                query = msg.get("content", "")
                break

        if not query:
            raise ValueError("User query is missing")

        # 2. 컨텍스트 구성 및 메타데이터 수집
        context_text = ""
        case_infos = []  # (title, duration_seconds, duration_str) 리스트
        
        # [Path A] 특정 영상에 대한 상세 가이드 요청
        target_video_id = video_id
        if not target_video_id:
            ref_keywords = ["저 영상", "그 영상", "추천해준", "이거", "그거", "어떻게 만드", "제작", "방법", "알려"]
            if any(k in query for k in ref_keywords):
                for msg in reversed(user_messages):
                    if msg.get("role") == "assistant":
                        videos = msg.get("videos")
                        if videos and isinstance(videos, list) and len(videos) > 0:
                            # 사용자 질문에서 영상 제목 키워드 매칭 시도
                            matched_video = self._match_video_by_title(query, videos)
                            if matched_video:
                                target_video_id = matched_video.get("video_id")
                                logger.info(f"[GuideChatUseCase] 제목 매칭으로 영상 ID 추출: {target_video_id} (제목: {matched_video.get('title')})")
                            else:
                                # 매칭 실패 시 첫 번째 영상 선택
                                target_video_id = videos[0].get("video_id")
                                logger.info(f"[GuideChatUseCase] 제목 매칭 실패, 첫 번째 영상 선택: {target_video_id}")
                            break

        if target_video_id and target_video_id != "all":
            logger.info(f"[GuideChatUseCase] Path A - 특정 영상 가이드 요청: {target_video_id}")
            analysis = await self.video_repository.get_analysis(target_video_id)
            if analysis:
                duration_sec = self._parse_duration_to_seconds(analysis.video_duration)
                case_infos.append((analysis.video_title or "제목 없음", duration_sec, analysis.video_duration))
                context_text = self._build_structural_summary(analysis, case_number=1)
                logger.info(f"[GuideChatUseCase] 영상 ID: {target_video_id}, 제목: {analysis.video_title or '제목없음'}, 길이: {analysis.video_duration} - DB 데이터 로드 성공")
            else:
                context_text = f"요청하신 영상(ID: {target_video_id})의 분석 데이터를 찾을 수 없습니다."
                logger.warning(f"[GuideChatUseCase] 영상 ID: {target_video_id} - DB 분석 데이터 없음")
        
        # [Path B] 제작 방법 패턴 분석 (전체 영상 구조 기반)
        else:
            query_embedding = await self.embedding_generator.generate_embedding(query)
            similar_chunks = await self.embedding_repository.search_similar(
                query_embedding=query_embedding,
                limit=limit
            )
            
            logger.info(f"[GuideChatUseCase] 쿼리: '{query}'")
            logger.info(f"[GuideChatUseCase] 유사 청크 검색 결과: {len(similar_chunks)}개")
            
            video_scores = {}
            for c in similar_chunks:
                vid = c.get('video_id')
                if vid:
                    video_scores[vid] = video_scores.get(vid, 0) + c.get('similarity', 0)
            
            top_video_ids = sorted(video_scores.keys(), key=lambda v: video_scores[v], reverse=True)[:3]
            
            logger.info(f"[GuideChatUseCase] 상위 영상 ID 및 점수: {[(vid, video_scores[vid]) for vid in top_video_ids]}")
            
            if not top_video_ids:
                context_text = "관련된 영상을 찾을 수 없어 제작 가이드를 제공하기 어렵습니다."
                logger.warning("[GuideChatUseCase] 관련 영상을 찾지 못함 - DB 분석 데이터 미사용")
            else:
                full_contexts = []
                for idx, vid in enumerate(top_video_ids, start=1):
                    analysis = await self.video_repository.get_analysis(vid)
                    if analysis:
                        duration_sec = self._parse_duration_to_seconds(analysis.video_duration)
                        case_infos.append((analysis.video_title or "제목 없음", duration_sec, analysis.video_duration))
                        summary = self._build_structural_summary(analysis, case_number=idx)
                        full_contexts.append(summary)
                        logger.info(f"[GuideChatUseCase] 사례{idx} - video_id: {vid}, 제목: {analysis.video_title or '제목없음'}, 길이: {analysis.video_duration} - DB 데이터 로드 성공")
                    else:
                        logger.warning(f"[GuideChatUseCase] 사례{idx} - video_id: {vid} - DB 분석 데이터 없음")
                
                if full_contexts:
                    context_text = "\n\n".join(full_contexts)
                    logger.info(f"[GuideChatUseCase] 총 {len(full_contexts)}개 사례의 분석 데이터 사용")
                else:
                    context_text = "영상 ID는 식별되었으나 분석 데이터가 없어 제작 가이드를 제공하기 어렵습니다."
                    logger.warning("[GuideChatUseCase] 모든 영상의 분석 데이터 조회 실패 - DB 분석 데이터 미사용")
            
        logger.info(f"[GuideChatUseCase] 최종 컨텍스트 길이: {len(context_text)} chars")
        
        # 3. 동적 시스템 프롬프트 생성
        system_prompt = self._build_dynamic_prompt(case_infos)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": f"### 벤치마킹 대상 영상 분석 데이터:\n{context_text}"},
        ] + user_messages

        # 4. OpenAI Completion 생성
        stream = self.client.chat.completions.create(
            model=self.settings.model or "gpt-4o",
            messages=messages,
            stream=True
        )

        return stream

    def _match_video_by_title(self, query: str, videos: List[dict]) -> Optional[dict]:
        """사용자 질문에서 영상 제목 키워드를 매칭하여 해당 영상 반환"""
        query_lower = query.lower()
        
        best_match = None
        best_score = 0
        
        for video in videos:
            title = video.get("title", "")
            if not title:
                continue
            
            # 제목을 키워드로 분리 (공백, 특수문자 기준)
            title_keywords = re.split(r'[\s\-\[\]\(\):\|,]+', title.lower())
            title_keywords = [k for k in title_keywords if len(k) >= 2]  # 2글자 이상만
            
            # 쿼리에 포함된 키워드 수 계산
            match_count = sum(1 for kw in title_keywords if kw in query_lower)
            
            # 가장 많이 매칭된 영상 선택 (최소 1개 이상 매칭 필요)
            if match_count > best_score:
                best_score = match_count
                best_match = video
                
        if best_match:
            logger.info(f"[GuideChatUseCase] 제목 매칭 성공: '{best_match.get('title')}' (매칭 키워드 수: {best_score})")
        
        return best_match

    def _build_dynamic_prompt(self, case_infos: List[Tuple[str, Optional[int], Optional[str]]]) -> str:
        """사례 정보를 기반으로 동적인 시스템 프롬프트 생성"""
        case_count = len(case_infos)
        
        if case_count == 0:
            return (
                "당신은 '영상 콘텐츠 제작 전략가'입니다. "
                "사용자의 질문에 일반적인 영상 제작 가이드를 제공해 주세요. "
                "단, 현재 분석된 사례 데이터가 없으므로 일반론적인 조언만 가능함을 안내하세요."
            )
        
        # 영상 유형 판단 (첫 번째 사례 기준, 또는 평균)
        durations = [d for (_, d, _) in case_infos if d is not None]
        avg_duration = sum(durations) / len(durations) if durations else None
        video_type = self._classify_video_type(avg_duration)
        
        # 사례 목록 생성
        case_list = "\n".join([
            f"  - 사례 {i+1}: \"{title}\" (길이: {dur_str or '알 수 없음'})"
            for i, (title, _, dur_str) in enumerate(case_infos)
        ])
        
        return f"""당신은 '{video_type} 콘텐츠 제작 전략가'입니다.

⚠️ **중요 제약사항**:
- 제공된 분석 데이터는 **총 {case_count}개 사례**입니다:
{case_list}
- **반드시 제공된 사례만 참조**하세요. 존재하지 않는 "사례 {case_count + 1}" 등을 언급하지 마세요.
- 사례를 인용할 때 **"사례 1 (영상제목)"** 형태로 제목도 함께 명시하세요.

📋 **답변 가이드**:
1. **영상 유형 맞춤 조언**: 이 영상은 {video_type}입니다. 해당 유형에 적합한 구조와 연출을 제안하세요.
2. **구조적 패턴 분석**: 도입부, 본론, 결말의 흐름을 분석하세요.
3. **구체적 실행 가이드**: 시간대별 구체적인 지침을 주세요 (예: "0~10초: 훅으로 시작").
4. **근거 제시**: 제공된 사례 데이터를 인용하여 근거를 명확히 하세요.
5. **전문가 어조**: 크리에이터가 바로 촬영에 들어갈 수 있도록 명확한 지침을 주세요."""

    def _build_structural_summary(self, analysis: VideoAnalysisResult, case_number: int = None) -> str:
        """영상 전체의 구조적 특징을 요약하여 프롬프트 컨텍스트 생성"""
        title_info = f" ({analysis.video_title})" if analysis.video_title else ""
        duration_info = f" [길이: {analysis.video_duration}]" if analysis.video_duration else ""
        
        # 사례 번호가 있으면 "사례 N" 형태로 표시, 없으면 기존 형태 유지
        if case_number:
            header = f"=== 사례 {case_number}:{title_info}{duration_info} ==="
        else:
            header = f"=== 영상 ID: {analysis.video_id}{title_info}{duration_info} 분석 데이터 ==="
        
        lines = [header]
        
        # 1. 타임라인 기반 구조 (대본 + 시각)
        lines.append("[타임라인별 전개 및 연출]")
        
        segments = analysis.transcript_segments
        frames = analysis.visual_frames
        
        # 스크립트와 시각 정보를 시간순으로 병합하여 전개 흐름 구성
        # 30초 단위로 묶어서 흐름 파악 (토큰 절약 및 구조화)
        duration = segments[-1].end if segments else 0
        interval = 30.0 
        
        current_time = 0.0
        while current_time < duration:
            end_time = current_time + interval
            
            # 해당 구간 스크립트 요약
            texts = [s.text for s in segments if s.start >= current_time and s.start < end_time]
            section_text = " ".join(texts)
            if len(section_text) > 200: section_text = section_text[:200] + "..." # 너무 길면 자름
            
            # 해당 구간 시각 정보 요약 (주요 객체)
            section_frames = [f for f in frames if f.timestamp >= current_time and f.timestamp < end_time]
            objects = []
            for f in section_frames:
                 objects.extend([o.class_name for o in f.objects])
            
            # 가장 많이 등장한 객체 top 3
            from collections import Counter
            common_objects = [obj for obj, _ in Counter(objects).most_common(3)]
            
            if section_text or common_objects:
                visual_desc = f", 주요 시각요소: {', '.join(common_objects)}" if common_objects else ""
                lines.append(f"- {current_time:.0f}s~{end_time:.0f}s: (내용) {section_text}{visual_desc}")
            
            current_time += interval
            
        return "\n".join(lines)
