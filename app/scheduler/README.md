# YouTube 트렌드 스케줄러 시스템

## 개요

기존의 절차적 배치 시스템(`trend_batch.py`, `youtube_tag_batch.py`)을 대체하는 **객체 지향적 스케줄러 시스템**입니다.

## 주요 기능

### 🔄 통합된 기능들
- **영상 수집**: 선별된 YouTube 영상 자동 수집
- **트렌드 분석**: 급등 영상 탐지 및 점수 계산  
- **메트릭 스냅샷**: 일별 영상 메트릭 스냅샷 생성
- **트렌드 집계**: 카테고리별 트렌드 데이터 집계
- **태그 분석**: 카테고리별 인기 태그 집계

### 🏗️ 아키텍처

```
app/scheduler/
├── core/           # 베이스 클래스 & 인터페이스
│   ├── interfaces.py     # 추상 인터페이스
│   ├── base_scheduler.py # 스케줄러 베이스 클래스
│   └── events.py         # 이벤트 시스템
├── strategies/     # 수집 전략 (Strategy Pattern)
│   └── selective_video_strategy.py
├── analyzers/      # 트렌드 분석 엔진  
│   └── trend_analyzer.py
├── config/         # 설정 관리
│   └── scheduler_config.py
├── managers/       # 팩토리 & 매니저
│   └── scheduler_manager.py
├── api/           # 모니터링 API
│   └── scheduler_router.py
└── youtube_trend_scheduler.py  # 메인 서비스
```

## 사용법

### 환경 변수 설정

```bash
# 스케줄러 활성화/비활성화
ENABLE_YOUTUBE_TREND_SCHEDULER=true

# 수집 주기 (분 단위)
SCHEDULER_INTERVAL_MINUTES=30

# 수집 설정
MAX_VIDEOS_PER_CYCLE=50
MIN_VIEW_COUNT=1000
MIN_LIKE_COUNT=10
MAX_VIDEO_AGE_HOURS=24

# 트렌드 분석 설정
MIN_TREND_SCORE=0.7
ANALYSIS_WINDOW_HOURS=8

# 가중치 설정 (합계 1.0)
VIEW_GROWTH_WEIGHT=0.4
LIKE_GROWTH_WEIGHT=0.3
COMMENT_GROWTH_WEIGHT=0.2
VELOCITY_WEIGHT=0.1
```

### API 모니터링

```bash
# 전체 스케줄러 상태 확인
GET /api/v1/schedulers/status

# 특정 스케줄러 제어
POST /api/v1/schedulers/mvp_youtube_scheduler/start
POST /api/v1/schedulers/mvp_youtube_scheduler/stop

# 스케줄러 목록 조회
GET /api/v1/schedulers
```

## MVP 요구사항 준수

- ✅ **30분 주기** 데이터 수집
- ✅ **선별된 영상만** 추적 (급등 후보, 트렌드 영상)  
- ✅ **8시간 기준** 급등 영상 탐지
- ✅ **카테고리별** 분석 (음악, 엔터테인먼트, 뉴스 등)
- ✅ **복합 점수** 기반 트렌드 판단
- ✅ **한국 지역** YouTube 영상 중심

## 기존 시스템과의 차이점

| 기존 시스템 | 새 시스템 |
|------------|----------|
| 절차적 코드 | 객체 지향 설계 |
| 하드코딩된 설정 | 환경변수 기반 설정 |
| 단순 배치 실행 | 이벤트 기반 처리 |
| 에러 처리 부족 | 강화된 에러 처리 |
| 모니터링 불가 | API 기반 모니터링 |
| 확장성 낮음 | Strategy 패턴으로 확장 용이 |

## 주요 디자인 패턴

- **Strategy Pattern**: 다양한 수집 전략 지원
- **Factory Pattern**: 설정 기반 스케줄러 생성
- **Observer Pattern**: 이벤트 기반 알림 시스템
- **Singleton Pattern**: 서비스 인스턴스 관리

## 트렌드 점수 계산

영상의 트렌드 점수는 다음 요소들의 가중 평균으로 계산됩니다:

1. **조회수 증가율** (40%): 시간당 조회수 vs 카테고리 평균
2. **좋아요 증가율** (30%): 좋아요 비율 vs 일반 기준
3. **댓글 증가율** (20%): 댓글 비율 vs 일반 기준  
4. **속도 지표** (10%): 최근 업로드 영상의 급등 정도

## 확장 계획

- 다른 플랫폼 지원 (Instagram, TikTok)
- 실시간 트렌드 탐지
- 머신러닝 기반 예측 모델 통합
- 더 정교한 트렌드 분석 알고리즘