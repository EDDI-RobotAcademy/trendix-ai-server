import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from collections import defaultdict

from content.domain.video import Video
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from ..core.interfaces import TrendAnalyzerInterface, TrendAnalysisResult
from ..config.scheduler_config import TrendAnalysisConfig, TrendAnalysisMethod


logger = logging.getLogger(__name__)


class CompositeTrendAnalyzer(TrendAnalyzerInterface):
    """복합 지표 기반 트렌드 분석 엔진 (MVP 요구사항 기준)"""
    
    def __init__(self, config: TrendAnalysisConfig):
        self.config = config
        self.repository = ContentRepositoryImpl()
    
    async def analyze_trends(self, videos: List[Video]) -> TrendAnalysisResult:
        """트렌드 분석 수행"""
        start_time = datetime.now()
        
        if not videos:
            return TrendAnalysisResult(
                trending_videos=[],
                analysis_metrics={},
                category_trends={},
                timestamp=start_time
            )
        
        # 1. 각 영상의 트렌드 점수 계산
        scored_videos = []
        for video in videos:
            score = self.calculate_trend_score(video)
            if score >= self.config.min_trend_score:
                scored_videos.append((video, score))
        
        # 2. 점수 순으로 정렬
        scored_videos.sort(key=lambda x: x[1], reverse=True)
        trending_videos = [video for video, _ in scored_videos]
        
        # 3. 카테고리별 트렌드 분석
        category_trends = self._analyze_category_trends(trending_videos)
        
        # 4. 전체 분석 메트릭 계산
        analysis_metrics = self._calculate_analysis_metrics(videos, trending_videos)
        
        logger.info(f"Analyzed {len(videos)} videos, found {len(trending_videos)} trending")
        
        return TrendAnalysisResult(
            trending_videos=trending_videos,
            analysis_metrics=analysis_metrics,
            category_trends=category_trends,
            timestamp=start_time
        )
    
    def calculate_trend_score(self, video: Video) -> float:
        """
        영상의 트렌드 점수 계산 (MVP 요구사항 기준)
        
        점수 구성:
        - 조회수 증가율 (40%)
        - 좋아요 증가율 (30%) 
        - 댓글 증가율 (20%)
        - 속도 지표 (10%)
        """
        try:
            if self.config.method == TrendAnalysisMethod.COMPOSITE_SCORE:
                return self._calculate_composite_score(video)
            elif self.config.method == TrendAnalysisMethod.GROWTH_RATE:
                return self._calculate_growth_rate_score(video)
            elif self.config.method == TrendAnalysisMethod.VELOCITY_BASED:
                return self._calculate_velocity_score(video)
            else:
                return 0.0
                
        except Exception as e:
            logger.error(f"Failed to calculate trend score for {video.video_id}: {e}")
            return 0.0
    
    def _calculate_composite_score(self, video: Video) -> float:
        """복합 점수 계산"""
        scores = []
        
        # 1. 조회수 기반 점수
        view_score = self._calculate_view_growth_score(video)
        scores.append(view_score * self.config.view_growth_weight)
        
        # 2. 좋아요 기반 점수  
        like_score = self._calculate_like_growth_score(video)
        scores.append(like_score * self.config.like_growth_weight)
        
        # 3. 댓글 기반 점수
        comment_score = self._calculate_comment_growth_score(video)
        scores.append(comment_score * self.config.comment_growth_weight)
        
        # 4. 속도 지표 점수
        velocity_score = self._calculate_velocity_indicator(video)
        scores.append(velocity_score * self.config.velocity_weight)
        
        return sum(scores)
    
    def _calculate_view_growth_score(self, video: Video) -> float:
        """조회수 증가율 점수 계산"""
        if not video.view_count or not video.published_at:
            return 0.0
            
        # 영상 나이 (시간 단위)
        age_hours = (datetime.now() - video.published_at).total_seconds() / 3600
        if age_hours <= 0:
            return 0.0
            
        # 시간당 조회수
        views_per_hour = video.view_count / age_hours
        
        # 카테고리별 평균과 비교하여 정규화
        category_avg = self._get_category_average_views_per_hour(video.category_id)
        if category_avg > 0:
            relative_score = min(views_per_hour / category_avg, 10.0)  # 최대 10배까지
            return math.log10(1 + relative_score) / math.log10(11)  # 0~1 정규화
        
        # 절대적 기준으로 점수 계산 (카테고리 평균이 없는 경우)
        return min(views_per_hour / 10000, 1.0)  # 시간당 1만뷰 = 1.0 점
    
    def _calculate_like_growth_score(self, video: Video) -> float:
        """좋아요 증가율 점수 계산"""
        if not video.like_count or not video.view_count or video.view_count == 0:
            return 0.0
            
        # 좋아요 비율
        like_ratio = video.like_count / video.view_count
        
        # 일반적인 좋아요 비율 대비 상대 점수 (유튜브 평균 약 3-5%)
        baseline_ratio = 0.03
        relative_ratio = min(like_ratio / baseline_ratio, 5.0)  # 최대 5배
        
        return min(relative_ratio / 5.0, 1.0)
    
    def _calculate_comment_growth_score(self, video: Video) -> float:
        """댓글 증가율 점수 계산"""
        if not video.comment_count or not video.view_count or video.view_count == 0:
            return 0.0
            
        # 댓글 비율
        comment_ratio = video.comment_count / video.view_count
        
        # 일반적인 댓글 비율 대비 상대 점수 (유튜브 평균 약 0.5-1%)
        baseline_ratio = 0.005
        relative_ratio = min(comment_ratio / baseline_ratio, 10.0)  # 최대 10배
        
        return min(relative_ratio / 10.0, 1.0)
    
    def _calculate_velocity_indicator(self, video: Video) -> float:
        """속도 지표 계산 (최근 급등 정도)"""
        if not video.published_at:
            return 0.0
            
        age_hours = (datetime.now() - video.published_at).total_seconds() / 3600
        
        # 최근 영상일수록 높은 점수 (8시간 이내 급등 감지가 목적)
        if age_hours <= self.config.analysis_window_hours:
            # 최근 영상일수록 가중치 증가
            time_factor = 1.0 - (age_hours / self.config.analysis_window_hours)
            
            # 조회수와 시간의 관계로 속도 계산
            if video.view_count and age_hours > 0:
                velocity = video.view_count / age_hours
                # 로그 스케일로 정규화
                return time_factor * min(math.log10(1 + velocity) / 6.0, 1.0)
        
        return 0.0
    
    def _calculate_growth_rate_score(self, video: Video) -> float:
        """증가율 기반 점수 (단순화된 버전)"""
        view_score = self._calculate_view_growth_score(video)
        like_score = self._calculate_like_growth_score(video)
        return (view_score + like_score) / 2.0
    
    def _calculate_velocity_score(self, video: Video) -> float:
        """속도 기반 점수"""
        return self._calculate_velocity_indicator(video)
    
    def _get_category_average_views_per_hour(self, category_id: int) -> float:
        """카테고리별 평균 시간당 조회수 조회"""
        try:
            if not category_id:
                return 10000.0  # 기본값
                
            # 최근 7일간 해당 카테고리 영상들의 평균 계산
            recent_videos = self.repository.get_recent_videos(hours=168, limit=100)  # 7일
            category_videos = [v for v in recent_videos if v.category_id == category_id]
            
            if not category_videos:
                return 10000.0
                
            total_views_per_hour = 0
            count = 0
            
            for video in category_videos:
                if video.view_count and video.published_at:
                    age_hours = (datetime.now() - video.published_at).total_seconds() / 3600
                    if age_hours > 0:
                        total_views_per_hour += video.view_count / age_hours
                        count += 1
            
            return total_views_per_hour / count if count > 0 else 10000.0
            
        except Exception as e:
            logger.error(f"Failed to get category average: {e}")
            return 10000.0
    
    def _analyze_category_trends(self, trending_videos: List[Video]) -> Dict[str, List[Video]]:
        """카테고리별 트렌드 분석"""
        category_trends = defaultdict(list)
        
        for video in trending_videos:
            if video.category_id:
                category_name = self._get_category_name(video.category_id)
                category_trends[category_name].append(video)
        
        # 각 카테고리별로 최대 5개까지만 유지
        for category in category_trends:
            category_trends[category] = category_trends[category][:5]
        
        return dict(category_trends)
    
    def _get_category_name(self, category_id: int) -> str:
        """카테고리 ID를 이름으로 변환"""
        category_map = {
            1: "영화/애니메이션",
            2: "자동차/탈것", 
            10: "음악",
            15: "애완동물/동물",
            17: "스포츠",
            19: "여행/이벤트",
            20: "게임",
            22: "인물/블로그",
            23: "코미디",
            24: "엔터테인먼트",
            25: "뉴스/정치",
            26: "하우투/스타일",
            27: "교육",
            28: "과학기술"
        }
        return category_map.get(category_id, f"카테고리_{category_id}")
    
    def _calculate_analysis_metrics(self, all_videos: List[Video], trending_videos: List[Video]) -> Dict[str, float]:
        """전체 분석 메트릭 계산"""
        if not all_videos:
            return {}
            
        total_videos = len(all_videos)
        trending_count = len(trending_videos)
        trending_ratio = trending_count / total_videos if total_videos > 0 else 0
        
        # 평균 점수 계산
        scores = [self.calculate_trend_score(video) for video in all_videos]
        avg_score = sum(scores) / len(scores) if scores else 0
        max_score = max(scores) if scores else 0
        
        # 카테고리 분포
        category_distribution = defaultdict(int)
        for video in trending_videos:
            if video.category_id:
                category_name = self._get_category_name(video.category_id)
                category_distribution[category_name] += 1
        
        return {
            "total_analyzed": float(total_videos),
            "trending_count": float(trending_count),
            "trending_ratio": trending_ratio,
            "avg_trend_score": avg_score,
            "max_trend_score": max_score,
            "category_count": float(len(category_distribution)),
            "analysis_window_hours": float(self.config.analysis_window_hours),
            "min_threshold": self.config.min_trend_score
        }