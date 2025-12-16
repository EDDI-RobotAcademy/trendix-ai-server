import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field

from account.application.usecase.account_usecase import AccountUseCase
from account.infrastructure.repository.account_repository_impl import AccountRepositoryImpl
from config.s3_client import get_s3_client, AWS_S3_BUCKET, build_s3_url

account_router = APIRouter(tags=["account"])
usecase = AccountUseCase(AccountRepositoryImpl())
s3_client = get_s3_client()


class UpdateAccountRequest(BaseModel):
    nickname: str | None = Field(default=None, max_length=255)
    bio: str | None = Field(default=None, max_length=1000)


class InterestRequest(BaseModel):
    interest: str = Field(min_length=1, max_length=100)


def _account_to_dict(account):
    return {
        "id": account.id,
        "email": account.email,
        "nickname": account.nickname,
        "bio": account.bio,
        "profile_image_url": account.profile_image_url,
        "created_at": account.created_at,
        "updated_at": account.updated_at,
    }


def _interest_to_dict(interest):
    return {
        "id": interest.id,
        "interest": interest.interest,
        "created_at": interest.created_at,
    }


@account_router.get("/{account_id}")
def get_account(account_id: int):
    account = usecase.get_account_by_id(account_id)

    print("------account------", account)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    interests = usecase.list_interests(account_id)
    print("------interests------", interests)
    return {
        "account": _account_to_dict(account),
        "interests": [_interest_to_dict(i) for i in interests],
    }


@account_router.patch("/{account_id}")
async def update_account(
    account_id: int,
    nickname: str | None = Form(default=None, max_length=255),
    bio: str | None = Form(default=None, max_length=1000),
    profile_image: UploadFile | None = File(default=None),
):
    profile_image_url: str | None = None

    if profile_image:
        if AWS_S3_BUCKET is None:
            raise HTTPException(status_code=500, detail="S3 bucket is not configured")
        # 고유한 경로로 저장
        print ("111111")
        ext = Path(profile_image.filename or "").suffix

        print ("2222222",ext)
        key = f"profile-images/{account_id}/{uuid.uuid4().hex}{ext}"
        print ("33333",key)

        try:
            s3_client.upload_fileobj(
                profile_image.file,
                AWS_S3_BUCKET,
                key,
                ExtraArgs={
                    "ContentType": profile_image.content_type or "application/octet-stream",
                    # 버킷이 ACL 차단(Bucket owner enforced)일 경우 AccessDenied가 발생하므로 ACL은 생략
                },
            )
            print("444444")
            profile_image_url = build_s3_url(key)
            print("55555",profile_image_url)
        except Exception as exc:  # boto3 예외 일괄 처리
            raise HTTPException(status_code=500, detail=f"Failed to upload image: {exc}")

    try:
        account = usecase.update_profile(
            account_id,
            nickname=nickname,
            bio=bio,
            profile_image_url=profile_image_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _account_to_dict(account)


@account_router.get("/{account_id}/interests")
def list_interests(account_id: int):
    try:
        interests = usecase.list_interests(account_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return [_interest_to_dict(i) for i in interests]


@account_router.post("/{account_id}/interests")
def add_interest(account_id: int, request: InterestRequest):
    try:
        interest = usecase.add_interest(account_id, request.interest.strip())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _interest_to_dict(interest)


@account_router.delete("/{account_id}/interests/{interest_id}")
def delete_interest(account_id: int, interest_id: int):
    try:
        usecase.delete_interest(account_id, interest_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"deleted": True}
