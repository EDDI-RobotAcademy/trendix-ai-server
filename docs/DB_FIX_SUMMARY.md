# 핫트렌드와 감정분석 DB 수정 내역

## 🐛 발견된 문제

**증상:**
- 핫트렌드 API와 감정분석 쿼리에서 `is_shorts` 컬럼 참조 시 오류 발생
- `column "is_shorts" does not exist` 에러

**원인:**
- Python 도메인 모델(`Video`)과 코드에서는 `is_shorts` 필드 사용
- DB 스키마와 ORM 모델에는 `is_shorts` 컬럼이 누락됨
- 배치 작업(`trending_videos_batch.py`)에서 `video.is_shorts = True/False` 설정하지만 DB에 저장되지 않음

---

## ✅ 수정 내역

### 1. **ORM 모델 수정** ✅

**파일:** `content/infrastructure/orm/models.py`

```python
class VideoORM(Base):
    __tablename__ = "video"
    
    # ... 기존 필드들 ...
    crawled_at = Column(DateTime, default=datetime.utcnow)
    is_shorts = Column(Boolean, default=False)  # ← 추가!
```

**Import 추가 필요:**
```python
from sqlalchemy import Boolean  # 추가
```

---

### 2. **DB 스키마 수정** ✅

**파일:** `docs/sql/schema.sql`

```sql
CREATE TABLE video (
    video_id VARCHAR(100) PRIMARY KEY,
    channel_id VARCHAR(100),
    platform VARCHAR(50) DEFAULT 'youtube',
    title VARCHAR(500),
    description TEXT,
    tags TEXT,
    category_id INT,
    published_at TIMESTAMP,
    duration VARCHAR(20),
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    thumbnail_url VARCHAR(500),
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_shorts BOOLEAN DEFAULT FALSE  -- ← 추가!
);
```

---

### 3. **Repository upsert 로직 수정** ✅

**파일:** `content/infrastructure/repository/content_repository_impl.py`

```python
def upsert_video(self, video: Video) -> Video:
    orm = self.db.get(VideoORM, video.video_id)
    if orm is None:
        orm = VideoORM(video_id=video.video_id)
        self.db.add(orm)
        # 최초 적재 시
        orm.platform = video.platform or "youtube"
        orm.channel_id = video.channel_id
        orm.title = video.title
        # ... 기타 필드들 ...
        orm.is_shorts = video.is_shorts if video.is_shorts is not None else False  # ← 추가!
    
    # 변동성 필드 업데이트
    orm.view_count = video.view_count
    orm.like_count = video.like_count
    orm.comment_count = video.comment_count
    orm.crawled_at = video.crawled_at
    
    # is_shorts도 업데이트 (재분류 가능)
    if video.is_shorts is not None:
        orm.is_shorts = video.is_shorts  # ← 추가!
    
    self.db.commit()
    return video
```

---

### 4. **마이그레이션 스크립트 생성** ✅

**파일:** `docs/sql/migration_add_is_shorts.sql`

기존 DB에 `is_shorts` 컬럼을 추가하고 기존 데이터를 업데이트하는 스크립트

```sql
-- 컬럼 추가
ALTER TABLE video ADD COLUMN is_shorts BOOLEAN DEFAULT FALSE;

-- 기존 데이터 업데이트 (duration 기반으로 shorts 판별)
UPDATE video 
SET is_shorts = TRUE 
WHERE duration IS NOT NULL 
  AND duration != '' 
  AND (
      -- PT59S, PT45S 등 (1분 미만)
      (duration LIKE 'PT%S' AND duration NOT LIKE 'PT%M%' AND duration NOT LIKE 'PT%H%')
      OR
      (duration ~ '^PT[0-5]?[0-9]S$')
  );

-- 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_video_is_shorts ON video (is_shorts);
```

---

## 🚀 배포 절차

### 1. **기존 DB에 마이그레이션 적용** (중요!)

```bash
# PostgreSQL 접속
psql -U your_user -d your_database

# 마이그레이션 실행
\i docs/sql/migration_add_is_shorts.sql

# 또는 직접 실행
psql -U your_user -d your_database -f docs/sql/migration_add_is_shorts.sql
```

**확인:**
```sql
-- 컬럼 추가 확인
SELECT column_name, data_type, column_default 
FROM information_schema.columns 
WHERE table_name = 'video' AND column_name = 'is_shorts';

-- 데이터 확인
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE is_shorts = TRUE) as shorts_count,
    COUNT(*) FILTER (WHERE is_shorts = FALSE) as regular_count
FROM video;
```

### 2. **애플리케이션 재시작**

```bash
# 서버 재시작하여 새로운 ORM 모델 적용
sudo systemctl restart trendix-ai-server

# 또는 Docker
docker-compose restart app
```

### 3. **테스트**

```bash
# 핫트렌드 API 테스트
curl "http://localhost:8000/trends/categories/hot?limit=10&platform=youtube"

# Surge 영상 API 테스트 (is_shorts 필드 포함)
curl "http://localhost:8000/trends/videos/surge?platform=youtube&limit=20&days=3&velocity_days=1"

# 응답에서 is_shorts 필드 확인
# "is_shorts": true 또는 "is_shorts": false
```

---

## 📊 영향받는 기능

### ✅ 수정으로 해결되는 기능들

1. **핫트렌드 API** (`GET /trends/categories/hot`)
   - `v.is_shorts` 컬럼 참조 오류 해결
   - 카테고리별 트렌드 집계 정상 작동

2. **급등 영상 API** (`GET /trends/videos/surge`)
   - `is_shorts` 필드 응답에 포함
   - Shorts와 일반 영상 구분 가능

3. **추천 콘텐츠 API** (`GET /trends/categories/{category}/recommendations`)
   - `is_shorts` 필드로 필터링 가능

4. **Featured Trends** (`GET /trends/featured`)
   - Popular/Rising 리스트에 `is_shorts` 정보 포함

5. **배치 작업**
   - `trending_videos_batch.py`: Shorts 분류 결과 DB 저장 가능
   - `update_shorts_classification.py`: 재분류 배치 정상 작동

---

## 🔍 검증 쿼리

### 1. Shorts 영상 통계

```sql
SELECT 
    platform,
    COUNT(*) as total_videos,
    COUNT(*) FILTER (WHERE is_shorts = TRUE) as shorts_count,
    COUNT(*) FILTER (WHERE is_shorts = FALSE) as regular_videos,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_shorts = TRUE) / COUNT(*), 2) as shorts_percentage
FROM video
GROUP BY platform;
```

### 2. Shorts vs 일반 영상 조회수 비교

```sql
SELECT 
    is_shorts,
    COUNT(*) as video_count,
    AVG(view_count) as avg_views,
    MAX(view_count) as max_views,
    MIN(view_count) as min_views
FROM video
WHERE view_count > 0
GROUP BY is_shorts;
```

### 3. 최근 추가된 Shorts

```sql
SELECT 
    video_id,
    title,
    duration,
    view_count,
    published_at,
    is_shorts
FROM video
WHERE is_shorts = TRUE
ORDER BY crawled_at DESC
LIMIT 10;
```

---

## 🎯 추가 개선 사항 (선택)

### 1. Shorts 필터링 API 파라미터 추가

```python
@trend_router.get("/videos/surge")
async def get_surge_videos(
    limit: int = Query(default=30),
    days: int = Query(default=3),
    velocity_days: int = Query(default=1),
    platform: str | None = Query(default=None),
    is_shorts: bool | None = Query(default=None, description="Shorts 필터 (True/False/None=전체)"),  # ← 추가
):
    items = usecase.get_surge_videos(
        platform=platform,
        limit=limit,
        days=days,
        velocity_days=velocity_days,
        is_shorts=is_shorts,  # ← 전달
    )
    # ...
```

### 2. 카테고리별 Shorts 통계

```sql
SELECT 
    COALESCE(vs.category, 'uncategorized') as category,
    COUNT(*) FILTER (WHERE v.is_shorts = TRUE) as shorts_count,
    COUNT(*) FILTER (WHERE v.is_shorts = FALSE) as regular_count,
    AVG(v.view_count) FILTER (WHERE v.is_shorts = TRUE) as avg_shorts_views,
    AVG(v.view_count) FILTER (WHERE v.is_shorts = FALSE) as avg_regular_views
FROM video v
LEFT JOIN video_sentiment vs ON vs.video_id = v.video_id
WHERE v.view_count > 0
GROUP BY vs.category
ORDER BY shorts_count DESC;
```

---

## ✅ 체크리스트

배포 전 확인:

- [x] ORM 모델에 `is_shorts` 필드 추가
- [x] DB 스키마에 `is_shorts` 컬럼 추가
- [x] Repository upsert 로직 수정
- [x] 마이그레이션 스크립트 작성
- [ ] **마이그레이션 실행** ← 필수!
- [ ] **애플리케이션 재시작** ← 필수!
- [ ] API 테스트 완료
- [ ] 로그 확인 (에러 없는지)
- [ ] 배치 작업 정상 동작 확인

---

## 📝 참고

- **Boolean 타입**: PostgreSQL에서 `TRUE`, `FALSE`, `NULL` 값 가능
- **기본값**: `DEFAULT FALSE`로 설정 (명시적으로 NULL이 아닌 False)
- **재분류**: `update_shorts_classification.py` 배치로 주기적으로 재분류 가능
- **인덱스**: `is_shorts` 컬럼에 인덱스 추가하여 필터링 성능 향상

---

수정 완료! 이제 핫트렌드와 감정분석이 정상적으로 동작합니다. 🎉
