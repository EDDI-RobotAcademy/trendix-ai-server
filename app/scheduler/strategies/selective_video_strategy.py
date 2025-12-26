import logging
from datetime import datetime, timedelta, timezone
from typing import List, Set
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from content.domain.video import Video
from content.infrastructure.client.youtube_client import YouTubeClient
from content.infrastructure.repository.content_repository_impl import ContentRepositoryImpl
from config.settings import YouTubeSettings
from ..core.interfaces import VideoCollectorInterface, CollectionResult
from ..config.scheduler_config import CollectionConfig


logger = logging.getLogger(__name__)


class SelectiveVideoCollectionStrategy(VideoCollectorInterface):
    """선별된 영상만 수집하는 전략 (MVP 요구사항 기준)"""
    
    def __init__(self, config: CollectionConfig):
        self.config = config
        self.youtube_client = YouTubeClient(YouTubeSettings())
        self.repository = ContentRepositoryImpl()
        self._collected_video_ids: Set[str] = set()
        
    async def collect_videos(self, max_videos: int = None) -> CollectionResult:
        """
        MVP 요구사항에 따라 선별된 영상을 수집
        - 급등 후보 영상 (메인)
        - 상위 트렌드 영상  
        - 사용자가 조회/비교한 영상
        """
        start_time = datetime.now(timezone.utc)
        target_count = max_videos or self.config.max_videos_per_cycle
        
        collected_videos: List[Video] = []
        filtered_count = 0
        
        try:
            # 1. 급등 후보 영상 탐지 (MVP 핵심)
            surging_videos = await self._detect_surging_videos(target_count // 2)
            
            # 2. 상위 트렌드 영상 수집 (보완)
            trending_videos = await self._collect_trending_videos(target_count // 3)
            
            # 3. 기존에 추적 중인 영상들의 업데이트 
            tracked_videos = await self._update_tracked_videos()
            
            # 모든 영상 합치기 (급등 영상 우선순위)
            all_videos = surging_videos + trending_videos + tracked_videos
            
            # 4. 필터링 및 중복 제거
            for video in all_videos:
                if len(collected_videos) >= target_count:
                    break
                    
                if self.should_collect_video(video):
                    if video.video_id not in self._collected_video_ids:
                        collected_videos.append(video)
                        self._collected_video_ids.add(video.video_id)
                    else:
                        filtered_count += 1
                else:
                    filtered_count += 1
                    
            # 5. 데이터베이스에 저장
            for video in collected_videos:
                video.platform = "youtube"
                video.crawled_at = datetime.now(timezone.utc)
                self.repository.upsert_video(video)
                
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            logger.info(f"Collected {len(collected_videos)} videos, filtered {filtered_count}")
            
            return CollectionResult(
                videos=collected_videos,
                total_collected=len(collected_videos),
                filtered_count=filtered_count,
                execution_time=execution_time,
                timestamp=start_time,
                metadata={
                    "surging_count": len(surging_videos),
                    "trending_count": len(trending_videos), 
                    "tracked_count": len(tracked_videos),
                    "strategy": "mvp_selective_collection"
                }
            )
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return CollectionResult(
                videos=[],
                total_collected=0,
                filtered_count=filtered_count,
                execution_time=execution_time,
                timestamp=start_time,
                metadata={"error": str(e)}
            )
    
    def should_collect_video(self, video: Video) -> bool:
        """영상 수집 여부 판단"""
        # 기본 필터링 조건
        if not video.video_id or not video.title:
            return False
            
        # 최소 조회수/좋아요 체크
        if video.view_count and video.view_count < self.config.min_view_count:
            return False
            
        if video.like_count and video.like_count < self.config.min_like_count:
            return False
        
        # 영상 나이 체크
        if video.published_at:
            max_age = datetime.now(timezone.utc) - timedelta(hours=self.config.max_video_age_hours)
            if video.published_at < max_age:
                return False
                
        # 카테고리 체크
        if self.config.target_categories:
            # category_id가 None이거나 지정된 카테고리에 없으면 수집하지 않음
            if video.category_id is None or video.category_id not in self.config.target_categories:
                return False
                
        # 제외 채널 체크
        if video.channel_id in self.config.exclude_channels:
            return False
            
        # Shorts 전용 설정 체크 (duration 기반 추정)
        if self.config.include_shorts_only and video.duration:
            # YouTube Shorts는 보통 60초 이하
            if not self._is_likely_short(video.duration):
                return False
                
        return True
    
    async def _collect_trending_videos(self, max_count: int) -> List[Video]:
        """YouTube API를 통해 트렌딩 영상 수집"""
        try:
            service = self.youtube_client.service
            
            # 한국 지역의 트렌딩 영상 조회
            response = service.videos().list(
                part="snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode="KR",  # 한국
                maxResults=min(max_count, 50),
                videoCategoryId=None  # 모든 카테고리
            ).execute()
            
            videos = []
            for item in response.get("items", []):
                video = self._parse_youtube_video(item)
                if video:
                    videos.append(video)
                    
            logger.info(f"Collected {len(videos)} trending videos")
            return videos
            
        except HttpError as e:
            logger.error(f"Failed to collect trending videos: {e}")
            return []
    
    async def _collect_category_videos(self, max_count: int) -> List[Video]:
        """카테고리별 인기 영상 수집"""
        videos = []
        
        try:
            service = self.youtube_client.service
            
            # 빈 카테고리 리스트 처리
            if not self.config.target_categories:
                logger.info("No target categories specified, skipping category collection")
                return videos
                
            per_category = max_count // len(self.config.target_categories)
            
            for category_id in self.config.target_categories:
                response = service.videos().list(
                    part="snippet,statistics,contentDetails",
                    chart="mostPopular",
                    regionCode="KR",
                    videoCategoryId=str(category_id),
                    maxResults=min(per_category, 20)
                ).execute()
                
                for item in response.get("items", []):
                    video = self._parse_youtube_video(item)
                    if video:
                        videos.append(video)
                        
            logger.info(f"Collected {len(videos)} category videos")
            return videos
            
        except HttpError as e:
            logger.error(f"Failed to collect category videos: {e}")
            return []
    
    async def _update_tracked_videos(self) -> List[Video]:
        """기존 추적 중인 영상들의 메트릭 업데이트"""
        try:
            # 최근 24시간 내에 수집된 영상들 중 일부를 재수집하여 메트릭 업데이트
            recent_videos = self.repository.get_recent_videos(
                platform="youtube",
                hours=24,
                limit=20
            )
            
            video_ids = [v.video_id for v in recent_videos if v.video_id]
            if not video_ids:
                return []
                
            updated_videos = list(self.youtube_client.fetch_videos_for_ids(video_ids))
            logger.info(f"Updated {len(updated_videos)} tracked videos")
            
            return updated_videos
            
        except Exception as e:
            logger.error(f"Failed to update tracked videos: {e}")
            return []
    
    def _parse_youtube_video(self, item: dict) -> Video:
        """YouTube API 응답을 Video 도메인 객체로 변환"""
        try:
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            
            return Video(
                video_id=item["id"],
                channel_id=snippet["channelId"],
                platform="youtube",
                title=snippet.get("title", ""),
                description=snippet.get("description"),
                tags=",".join(snippet.get("tags", [])) if snippet.get("tags") else None,
                category_id=int(snippet.get("categoryId")) if snippet.get("categoryId") else None,
                published_at=self.youtube_client._parse_datetime(snippet.get("publishedAt")),
                duration=content.get("duration"),
                view_count=int(stats.get("viewCount", 0)),
                like_count=int(stats.get("likeCount", 0)) if stats.get("likeCount") else 0,
                comment_count=int(stats.get("commentCount", 0)) if stats.get("commentCount") else 0,
                thumbnail_url=(snippet.get("thumbnails", {}).get("high") or {}).get("url"),
                crawled_at=datetime.now(timezone.utc),
                is_shorts=self._is_likely_short(content.get("duration")) if content.get("duration") else None
            )
            
        except Exception as e:
            logger.error(f"Failed to parse video: {e}")
            return None
    
    def _is_likely_short(self, duration: str) -> bool:
        """YouTube duration 형식(PT1M30S)을 파싱하여 Shorts 여부 판단"""
        try:
            # PT1M30S 형식 파싱
            if not duration or not duration.startswith("PT"):
                return False
                
            duration = duration[2:]  # "PT" 제거
            
            total_seconds = 0
            
            # 시간 파싱 (H)
            if "H" in duration:
                hours, duration = duration.split("H")
                total_seconds += int(hours) * 3600
                
            # 분 파싱 (M)  
            if "M" in duration:
                minutes, duration = duration.split("M")
                total_seconds += int(minutes) * 60
                
            # 초 파싱 (S)
            if "S" in duration:
                seconds = duration.replace("S", "")
                total_seconds += int(seconds)
                
            # Shorts는 보통 60초 이하
            return total_seconds <= 60
            
        except Exception:
            return False
    
    async def _detect_surging_videos(self, max_count: int) -> List[Video]:
        """
        MVP 핵심: 급등 후보 영상 탐지
        - 최근 8시간 내 업로드된 영상 중
        - 조회수/좋아요 증가 속도가 빠른 영상 탐지
        """
        try:
            service = self.youtube_client.service
            
            # 1. 최근 업로드된 영상들 검색 (여러 키워드 조합)
            surge_candidates = []
            
            # 한국 트렌드 키워드로 검색
            trending_keywords = [
                "크리스마스", "연말", "새해", "트렌드", "인기", "화제", 
                "shorts", "브이로그", "리뷰", "챌린지", "레시피"
            ]
            
            for keyword in trending_keywords[:3]:  # 상위 3개 키워드만
                try:
                    response = service.search().list(
                        part="snippet",
                        q=keyword,
                        type="video",
                        regionCode="KR",
                        order="date",  # 최신순
                        publishedAfter=(datetime.now(timezone.utc) - timedelta(hours=8)).isoformat(),
                        maxResults=10
                    ).execute()
                    
                    video_ids = [item["id"]["videoId"] for item in response.get("items", [])]
                    if video_ids:
                        # 상세 정보 조회
                        details = service.videos().list(
                            part="snippet,statistics,contentDetails",
                            id=",".join(video_ids)
                        ).execute()
                        
                        for item in details.get("items", []):
                            video = self._parse_youtube_video(item)
                            if video and self._is_surge_candidate(video):
                                surge_candidates.append(video)
                                
                except Exception as e:
                    logger.warning(f"Failed to search for keyword '{keyword}': {e}")
                    continue
            
            # 2. 급등 점수로 정렬 (조회수 대비 업로드 시간 고려)
            surge_candidates.sort(key=lambda v: self._calculate_surge_score(v), reverse=True)
            
            # 3. 중복 제거 및 상위 영상 반환
            unique_videos = []
            seen_ids = set()
            
            for video in surge_candidates:
                if video.video_id not in seen_ids and len(unique_videos) < max_count:
                    unique_videos.append(video)
                    seen_ids.add(video.video_id)
            
            logger.info(f"Detected {len(unique_videos)} surging video candidates")
            return unique_videos
            
        except HttpError as e:
            logger.error(f"Failed to detect surging videos: {e}")
            return []
    
    def _is_surge_candidate(self, video: Video) -> bool:
        """급등 후보 영상인지 판단"""
        if not video.view_count or not video.published_at:
            return False
            
        # 8시간 이내 업로드
        hours_since_upload = (datetime.now(timezone.utc) - video.published_at).total_seconds() / 3600
        if hours_since_upload > 8:
            return False
            
        # 기본 성능 기준 (시간당 최소 조회수)
        min_views_per_hour = 500  # MVP 기준
        views_per_hour = video.view_count / max(hours_since_upload, 0.1)
        
        return views_per_hour >= min_views_per_hour
    
    def _calculate_surge_score(self, video: Video) -> float:
        """급등 점수 계산"""
        if not video.view_count or not video.published_at:
            return 0.0
            
        hours_since_upload = (datetime.now(timezone.utc) - video.published_at).total_seconds() / 3600
        if hours_since_upload <= 0:
            return 0.0
            
        # 시간당 조회수 (기본 점수)
        views_per_hour = video.view_count / hours_since_upload
        
        # 좋아요 비율 보정
        like_ratio = video.like_count / max(video.view_count, 1) if video.like_count else 0
        like_boost = 1 + (like_ratio * 10)  # 좋아요 비율에 따른 가산점
        
        # 기본 급등 점수 (Shorts 보정 제거, 구분만 함)
        return views_per_hour * like_boost