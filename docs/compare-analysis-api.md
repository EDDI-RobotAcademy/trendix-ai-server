# 콘텐츠 비교 분석 API 설계 (내 쇼츠 vs 급등 쇼츠)

## 목적
프론트엔드 `콘텐츠 비교 분석` 화면에서 **내 쇼츠**와 **급등 쇼츠**를 비교하고, KR5 유료 전환 관점의 AI 개선 제안을 제공하기 위한 API 설계 문서입니다.

## 엔드포인트
- Method: `POST`
- Path: `/analysis/shorts/compare`

## 요청 바디 (JSON)
```json
{
  "platform": "youtube",
  "my_short_url": "https://youtube.com/shorts/xxxx",
  "trend_short_url": "https://youtube.com/shorts/yyyy"
}
```

### 필드 설명
- `platform`: 현재는 `youtube`만 지원
- `my_short_url`: 내 쇼츠 URL 또는 video_id
- `trend_short_url`: 급등 쇼츠 URL 또는 video_id

## 응답 바디 (JSON)
```json
{
  "my_video": {
    "id": "...",
    "title": "...",
    "channel_name": "...",
    "thumbnail_url": "...",
    "duration_sec": 32,
    "format_label": "Shorts · 9:16 · 자막형",
    "published_ago": "3시간 전"
  },
  "trend_video": {
    "id": "...",
    "title": "...",
    "channel_name": "...",
    "thumbnail_url": "...",
    "duration_sec": 22,
    "format_label": "Shorts · 9:16 · 하이라이트형",
    "published_ago": "1시간 전"
  },
  "hook_comparison": {
    "my": {
      "opening_line": "...",
      "visual_cue": "...",
      "caption_style": "...",
      "pacing": "...",
      "hook_score": 62
    },
    "trend": {
      "opening_line": "...",
      "visual_cue": "...",
      "caption_style": "...",
      "pacing": "...",
      "hook_score": 86
    },
    "takeaways": ["...", "..."]
  },
  "format_comparison": {
    "my": {
      "duration_sec": 32,
      "aspect_ratio": "9:16",
      "cut_count": 6,
      "text_density": "중간 (초당 7~9자)",
      "audio_style": "내레이션 + 잔잔한 BGM"
    },
    "trend": {
      "duration_sec": 22,
      "aspect_ratio": "9:16",
      "cut_count": 11,
      "text_density": "높음 (초당 10~12자)",
      "audio_style": "비트 강조 BGM + 효과음"
    },
    "differences": ["...", "..."]
  },
  "reaction_comparison": {
    "my": {
      "views": 48200,
      "likes": 2900,
      "comments": 180,
      "like_rate": 6.0,
      "completion_rate": 54.2,
      "retention_3s": 71.5,
      "share_rate": 1.2
    },
    "trend": {
      "views": 190000,
      "likes": 14300,
      "comments": 920,
      "like_rate": 7.5,
      "completion_rate": 63.8,
      "retention_3s": 84.1,
      "share_rate": 2.6
    },
    "insights": ["...", "..."]
  },
  "ai_summary": {
    "headline": "KR5 유료 전환을 위해 첫 3초에서 결과를 먼저 보여주세요.",
    "action_items": ["...", "...", "..."],
    "next_experiment": "..."
  },
  "trust_signals": ["...", "...", "..."]
}
```

## 로직 가이드
- **훅 구조 비교**: 오프닝 1~3초 키워드, 비주얼 등장 시점, 자막 스타일, 템포 기반 점수화
- **길이/포맷 비교**: 길이, 컷 수, 텍스트 밀도, 오디오 스타일
- **반응 지표 비교**: 조회/좋아요/댓글 + 좋아요율, 완주율, 3초 유지율, 공유율
- **AI 요약**: KR5 유료 전환 관점 헤드라인 + 액션 3개 + 다음 실험 제안
- **신뢰 강화 장치**: 비교 기준(상위 퍼센타일), 교차 검증 지표, 일치 조건 등을 문자열로 반환

## 제한 사항
- **Shorts 전용 API**: 각 영상의 길이가 60초를 초과하면 400 에러를 반환합니다.
- **자동 수집**: DB에 영상이 없으면 YouTube API로 수집한 뒤 비교 분석을 진행합니다. (API 키 필요)

## 프론트 연결 위치
- `C:\Users\tmcch\IdeaProjects\trendix-frontend\app\compare\VideoCompareClient.tsx`

## 구현 참고
- 라우터: `content/adapter/input/web/compare_router.py`
- 유스케이스: `content/application/usecase/shorts_compare_usecase.py`

## 에러 응답 예시
```json
{
  "detail": "지원하지 않는 플랫폼입니다. (현재 youtube만 가능)"
}
```

```json
{
  "detail": "내 쇼츠는 60초 이하만 분석할 수 있습니다."
}
```

## 요청 예시

```bash
curl -X POST "http://localhost:33333/analysis/shorts/compare" ^
  -H "Content-Type: application/json" ^
  -d "{\"platform\":\"youtube\",\"my_short_url\":\"https://youtube.com/shorts/abcd1234\",\"trend_short_url\":\"https://youtube.com/shorts/efgh5678\"}"
```

```bash
curl -X POST "http://localhost:33333/analysis/shorts/compare" ^
  -H "Content-Type: application/json" ^
  -d "{\"platform\":\"youtube\",\"my_short_url\":\"abcd1234\",\"trend_short_url\":\"efgh5678\"}"
```
