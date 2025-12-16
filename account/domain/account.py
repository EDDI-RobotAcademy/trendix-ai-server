from typing import Optional
from datetime import datetime


class Account:
    def __init__(
        self,
        email: str,
        nickname: str,
        bio: Optional[str] = None,
        profile_image_url: Optional[str] = None,
    ):
        self.id: Optional[int] = None
        self.email = email
        self.nickname = nickname
        self.bio = bio
        self.profile_image_url = profile_image_url
        self.created_at: datetime = datetime.utcnow()
        self.updated_at: datetime = datetime.utcnow()

    def update_profile(
        self,
        nickname: Optional[str] = None,
        bio: Optional[str] = None,
        profile_image_url: Optional[str] = None,
    ):
        if nickname is not None:
            self.nickname = nickname
        if bio is not None:
            self.bio = bio
        if profile_image_url is not None:
            self.profile_image_url = profile_image_url
        self.updated_at = datetime.utcnow()
