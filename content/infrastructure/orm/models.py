from datetime import datetime, date
from sqlalchemy import Column, String, Text, BigInteger, Integer, DateTime, Date, DECIMAL, Boolean, PrimaryKeyConstraint, UniqueConstraint, text

from config.database.session import Base


class ChannelORM(Base):
    __tablename__ = "channel"

    channel_id = Column(String(100), primary_key=True)
    platform = Column(String(50), default="youtube")
    title = Column(String(255))
    description = Column(Text)
    country = Column(String(50))
    subscriber_count = Column(BigInteger)
    view_count = Column(BigInteger)
    video_count = Column(Integer)
    created_at = Column(DateTime)
    crawled_at = Column(DateTime, default=datetime.utcnow)


class CreatorAccountORM(Base):
    __tablename__ = "creator_account"

    account_id = Column(String(100), primary_key=True)
    platform = Column(String(50), primary_key=True)
    display_name = Column(String(255))
    username = Column(String(255))
    profile_url = Column(String(500))
    description = Column(Text)
    country = Column(String(50))
    follower_count = Column(BigInteger)
    post_count = Column(BigInteger)
    last_updated_at = Column(DateTime)
    crawled_at = Column(DateTime, default=datetime.utcnow)


class VideoORM(Base):
    __tablename__ = "video"

    video_id = Column(String(100), primary_key=True)
    channel_id = Column(String(100))
    platform = Column(String(50), default="youtube")
    title = Column(String(500))
    description = Column(Text)
    tags = Column(Text)
    category_id = Column(Integer)
    published_at = Column(DateTime)
    duration = Column(String(20))
    view_count = Column(BigInteger)
    like_count = Column(BigInteger)
    comment_count = Column(BigInteger)
    thumbnail_url = Column(String(500))
    crawled_at = Column(DateTime, default=datetime.utcnow)


class VideoCommentORM(Base):
    __tablename__ = "video_comment"

    comment_id = Column(String(100), primary_key=True)
    video_id = Column(String(100))
    platform = Column(String(50), default="youtube")
    author = Column(String(255))
    content = Column(Text)
    like_count = Column(Integer)
    published_at = Column(DateTime)


class VideoSentimentORM(Base):
    __tablename__ = "video_sentiment"

    video_id = Column(String(100), primary_key=True)
    platform = Column(String(50), default="youtube")
    category = Column(String(100))
    trend_score = Column(DECIMAL(5, 4))
    sentiment_label = Column(String(50))
    sentiment_score = Column(DECIMAL(5, 4))
    keywords = Column(Text)
    summary = Column(Text)
    analyzed_at = Column(DateTime, default=datetime.utcnow)


class CommentSentimentORM(Base):
    __tablename__ = "comment_sentiment"

    comment_id = Column(String(100), primary_key=True)
    platform = Column(String(50), default="youtube")
    sentiment_label = Column(String(50))
    sentiment_score = Column(DECIMAL(5, 4))
    analyzed_at = Column(DateTime, default=datetime.utcnow)


class KeywordTrendORM(Base):
    __tablename__ = "keyword_trend"

    keyword = Column(String(100), primary_key=True)
    date = Column(Date, primary_key=True)
    platform = Column(String(50), primary_key=True)
    search_volume = Column(Integer)
    search_volume_prev = Column(BigInteger)
    video_count = Column(Integer)
    video_count_prev = Column(Integer)
    avg_sentiment = Column(DECIMAL(5, 4))
    avg_trend = Column(DECIMAL(5, 4))
    avg_total_score = Column(DECIMAL(6, 3))
    growth_rate = Column(DECIMAL(18, 4))
    rank = Column(Integer)


class CategoryTrendORM(Base):
    __tablename__ = "category_trend"

    category = Column(String(100), primary_key=True)
    date = Column(Date, primary_key=True)
    platform = Column(String(50), primary_key=True)
    video_count = Column(Integer)
    video_count_prev = Column(Integer)
    avg_sentiment = Column(DECIMAL(5, 4))
    avg_trend = Column(DECIMAL(5, 4))
    avg_total_score = Column(DECIMAL(6, 3))
    search_volume = Column(BigInteger)
    search_volume_prev = Column(BigInteger)
    growth_rate = Column(DECIMAL(18, 4))
    rank = Column(Integer)


class KeywordMappingORM(Base):
    __tablename__ = "keyword_mapping"
    __table_args__ = (
        PrimaryKeyConstraint("video_id", "keyword", "platform", name="pk_keyword_mapping"),
    )

    # 서버 기본값(nextval)로 채워지도록 server_default 지정, 애플리케이션에서는 직접 값 지정하지 않는다.
    mapping_id = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('keyword_mapping_mapping_id_seq'::regclass)"),
    )
    video_id = Column(String(100))
    channel_id = Column(String(100))
    platform = Column(String(50), default="youtube")
    keyword = Column(String(100))
    weight = Column(DECIMAL(5, 4))


class VideoScoreORM(Base):
    __tablename__ = "video_score"

    video_id = Column(String(100), primary_key=True)
    platform = Column(String(50), default="youtube")
    engagement_score = Column(DECIMAL(6, 3))
    sentiment_score = Column(DECIMAL(6, 3))
    trend_score = Column(DECIMAL(6, 3))
    total_score = Column(DECIMAL(6, 3))
    updated_at = Column(DateTime, default=datetime.utcnow)


class CrawlLogORM(Base):
    __tablename__ = "crawl_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    target_type = Column(String(50))
    target_id = Column(String(100))
    status = Column(String(50))
    message = Column(Text)
    crawled_at = Column(DateTime, default=datetime.utcnow)


class VideoMetricsSnapshotORM(Base):
    __tablename__ = "video_metrics_snapshot"
    __table_args__ = (
        PrimaryKeyConstraint("video_id", "snapshot_date", "platform", name="pk_video_metrics_snapshot"),
    )

    video_id = Column(String(100))
    platform = Column(String(50), default="youtube")
    snapshot_date = Column(Date)
    view_count = Column(BigInteger)
    like_count = Column(BigInteger)
    comment_count = Column(BigInteger)
