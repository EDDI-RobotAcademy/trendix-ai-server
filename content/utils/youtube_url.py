from urllib.parse import parse_qs, urlparse


def parse_youtube_video_id(url: str) -> str | None:
    # 한국어 주석: 다양한 유튜브 URL 패턴에서 video_id를 추출한다.
    if not url:
        return None
    parsed = urlparse(url.strip())
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    if "youtu.be" in host:
        video_id = path.strip("/").split("/")[0]
        return video_id or None

    if "youtube.com" in host:
        if path.startswith("/shorts/"):
            return path.split("/shorts/", 1)[1].split("/")[0] or None
        if path.startswith("/watch"):
            return (parse_qs(parsed.query).get("v") or [None])[0]
        if path.startswith("/embed/"):
            return path.split("/embed/", 1)[1].split("/")[0] or None

    return None
