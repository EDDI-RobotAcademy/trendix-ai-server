"""
급등 영상의 채널 정보를 YouTube API로 가져와서 channel 테이블에 업데이트하는 스크립트
"""
import os
import sys
from datetime import datetime, timedelta

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from googleapiclient.discovery import build
from sqlalchemy import text
from config.database.session import engine

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

def get_channel_ids_without_info():
    """channel 테이블에 정보가 없는 채널 ID들을 조회"""
    with engine.begin() as conn:
        # 최근 급등 영상의 채널 ID 중 channel.title이 없는 것들
        result = conn.execute(text("""
            SELECT DISTINCT v.channel_id
            FROM video v
            LEFT JOIN channel ch ON ch.channel_id = v.channel_id
            WHERE v.platform = 'youtube'
              AND v.published_at >= CURRENT_DATE - INTERVAL '7 days'
              AND (ch.title IS NULL OR ch.channel_id IS NULL)
            LIMIT 50
        """))

        channel_ids = [row[0] for row in result]
        print(f"채널 정보가 없는 채널 {len(channel_ids)}개 발견")
        return channel_ids

def fetch_channel_info_from_youtube(channel_ids):
    """YouTube API로 채널 정보 조회"""
    if not YOUTUBE_API_KEY:
        print("Error: YOUTUBE_API_KEY가 설정되지 않았습니다.")
        return []

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

    channels_data = []

    # YouTube API는 한 번에 최대 50개까지 조회 가능
    for i in range(0, len(channel_ids), 50):
        batch_ids = channel_ids[i:i+50]

        try:
            response = youtube.channels().list(
                part='snippet,statistics',
                id=','.join(batch_ids)
            ).execute()

            for item in response.get('items', []):
                snippet = item['snippet']
                statistics = item['statistics']

                channel_data = {
                    'channel_id': item['id'],
                    'platform': 'youtube',
                    'title': snippet.get('title', ''),
                    'description': snippet.get('description', ''),
                    'country': snippet.get('country', ''),
                    'subscriber_count': int(statistics.get('subscriberCount', 0)),
                    'video_count': int(statistics.get('videoCount', 0)),
                    'view_count': int(statistics.get('viewCount', 0)),
                }

                channels_data.append(channel_data)
                print(f"  ✓ {channel_data['title']} (구독자: {channel_data['subscriber_count']:,})")

        except Exception as e:
            print(f"YouTube API 요청 실패: {e}")
            continue

    return channels_data

def insert_channel_info(channels_data):
    """channel 테이블에 채널 정보 삽입"""
    with engine.begin() as conn:
        for channel in channels_data:
            try:
                # UPSERT 쿼리 (중복 시 업데이트)
                conn.execute(text("""
                    INSERT INTO channel (
                        channel_id, platform, title, description, country,
                        subscriber_count, video_count, view_count, crawled_at
                    ) VALUES (
                        :channel_id, :platform, :title, :description, :country,
                        :subscriber_count, :video_count, :view_count, NOW()
                    )
                    ON CONFLICT (channel_id)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        country = EXCLUDED.country,
                        subscriber_count = EXCLUDED.subscriber_count,
                        video_count = EXCLUDED.video_count,
                        view_count = EXCLUDED.view_count,
                        crawled_at = EXCLUDED.crawled_at
                """), channel)

            except Exception as e:
                print(f"  ✗ 채널 {channel['channel_id']} 삽입 실패: {e}")
                continue

    print(f"\n✓ {len(channels_data)}개 채널 정보 업데이트 완료")

def main():
    print("=" * 60)
    print("급등 영상 채널 정보 업데이트 스크립트")
    print("=" * 60)

    # 1. 채널 정보가 없는 채널 ID 조회
    channel_ids = get_channel_ids_without_info()

    if not channel_ids:
        print("업데이트할 채널이 없습니다.")
        return

    print(f"\n채널 정보 조회 중...")
    print("-" * 60)

    # 2. YouTube API로 채널 정보 가져오기
    channels_data = fetch_channel_info_from_youtube(channel_ids)

    if not channels_data:
        print("조회된 채널 정보가 없습니다.")
        return

    print(f"\n채널 정보 저장 중...")
    print("-" * 60)

    # 3. channel 테이블에 삽입
    insert_channel_info(channels_data)

    print("=" * 60)
    print("완료!")
    print("=" * 60)

if __name__ == "__main__":
    main()