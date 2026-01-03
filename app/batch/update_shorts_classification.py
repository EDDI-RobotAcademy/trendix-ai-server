import re
from sqlalchemy import text
from config.database.session import SessionLocal


def parse_duration_to_seconds(duration: str) -> int:
    """
    YouTube API duration format (PT1M30S)을 초 단위로 변환합니다.
    """
    if not duration or not duration.startswith('PT'):
        return 0
    
    # PT1M30S, PT45S, PT1H2M3S 등의 형태를 파싱
    duration = duration[2:]  # PT 제거
    
    # 정규식으로 시, 분, 초 추출
    hours_match = re.search(r'(\d+)H', duration)
    minutes_match = re.search(r'(\d+)M', duration)
    seconds_match = re.search(r'(\d+)S', duration)
    
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    seconds = int(seconds_match.group(1)) if seconds_match else 0
    
    return hours * 3600 + minutes * 60 + seconds


def update_shorts_classification():
    """
    기존 영상들의 is_shorts 정보를 duration 기반으로 업데이트합니다.
    """
    with SessionLocal() as db:
        # duration이 있지만 is_shorts가 NULL인 영상들을 조회
        videos = db.execute(
            text("""
                SELECT video_id, duration 
                FROM video 
                WHERE duration IS NOT NULL 
                AND is_shorts IS NULL
                AND platform = 'youtube'
                LIMIT 1000
            """)
        ).mappings().all()
        
        print(f"Found {len(videos)} videos to update...")
        
        updated_shorts = 0
        updated_regular = 0
        
        for video in videos:
            video_id = video["video_id"]
            duration = video["duration"]
            
            try:
                # duration을 초 단위로 변환
                seconds = parse_duration_to_seconds(duration)
                
                # 60초 이하면 Shorts로 분류
                is_shorts = seconds <= 60 and seconds > 0
                
                # 데이터베이스 업데이트
                db.execute(
                    text("UPDATE video SET is_shorts = :is_shorts WHERE video_id = :video_id"),
                    {"is_shorts": is_shorts, "video_id": video_id}
                )
                
                if is_shorts:
                    updated_shorts += 1
                else:
                    updated_regular += 1
                    
            except Exception as e:
                print(f"Error processing video {video_id}: {e}")
                continue
        
        # 변경사항 커밋
        db.commit()
        
        print(f"Update completed!")
        print(f"- Shorts: {updated_shorts}")
        print(f"- Regular videos: {updated_regular}")
        print(f"- Total updated: {updated_shorts + updated_regular}")


if __name__ == "__main__":
    update_shorts_classification()