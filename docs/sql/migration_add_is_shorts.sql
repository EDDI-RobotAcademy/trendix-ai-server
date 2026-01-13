-- Migration: Add is_shorts column to video table
-- Date: 2026-01-12
-- Purpose: Fix hot trends and sentiment analysis queries

-- Add is_shorts column if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'video' AND column_name = 'is_shorts'
    ) THEN
        ALTER TABLE video ADD COLUMN is_shorts BOOLEAN DEFAULT FALSE;
        COMMENT ON COLUMN video.is_shorts IS 'YouTube Shorts 여부 (60초 이하)';
        
        -- Update existing data based on duration
        UPDATE video 
        SET is_shorts = TRUE 
        WHERE duration IS NOT NULL 
          AND duration != '' 
          AND (
              -- PT59S, PT45S 등 (1분 미만)
              (duration LIKE 'PT%S' AND duration NOT LIKE 'PT%M%' AND duration NOT LIKE 'PT%H%')
              OR
              -- PT1M 미만 형식
              (duration ~ '^PT[0-5]?[0-9]S$')
          );
        
        RAISE NOTICE 'Column is_shorts added to video table';
    ELSE
        RAISE NOTICE 'Column is_shorts already exists';
    END IF;
END $$;

-- Add index for faster shorts filtering
CREATE INDEX IF NOT EXISTS idx_video_is_shorts ON video (is_shorts);

-- Verify migration
SELECT 
    COUNT(*) as total_videos,
    COUNT(*) FILTER (WHERE is_shorts = TRUE) as shorts_count,
    COUNT(*) FILTER (WHERE is_shorts = FALSE) as regular_count
FROM video;
