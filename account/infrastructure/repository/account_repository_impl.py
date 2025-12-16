from typing import List, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from account.application.port.account_repository_port import AccountRepositoryPort
from account.domain.account import Account
from account.domain.interest import AccountInterest
from account.infrastructure.orm.account_orm import AccountORM, AccountInterestORM
from config.database.session import get_db_session


class AccountRepositoryImpl(AccountRepositoryPort):
    def __init__(self):
        self.db: Session = get_db_session()

    def save(self, account: Account) -> Account:
        orm_account = AccountORM(
            email=account.email,
            nickname=account.nickname,
        )
        # 컬럼이 DB에 아직 없을 수 있으니 getattr/setattr로 안전하게 처리
        if hasattr(orm_account, "bio"):
            orm_account.bio = account.bio
        if hasattr(orm_account, "profile_image_url"):
            orm_account.profile_image_url = account.profile_image_url
        self.db.add(orm_account)
        self.db.commit()
        self.db.refresh(orm_account)
        return self._to_domain(orm_account)

    def update(self, account: Account) -> Account:
        orm_account: Optional[AccountORM] = self.db.get(AccountORM, account.id)
        if orm_account is None:
            raise ValueError(f"Account id={account.id} not found")
        orm_account.nickname = account.nickname
        if hasattr(orm_account, "bio"):
            orm_account.bio = account.bio
        if hasattr(orm_account, "profile_image_url"):
            orm_account.profile_image_url = account.profile_image_url
        self.db.commit()
        self.db.refresh(orm_account)
        return self._to_domain(orm_account)

    def find_by_id(self, account_id: int) -> Account | None:
        orm_account = self.db.get(AccountORM, account_id)
        if orm_account is None:
            return None
        return self._to_domain(orm_account)

    def find_by_email(self, email: str) -> Account | None:
        orm_account = self.db.query(AccountORM).filter(AccountORM.email == email).first()
        if orm_account is None:
            return None
        return self._to_domain(orm_account)

    def find_all_by_id(self, ids: list[int]) -> List[Account]:
        orm_accounts = self.db.query(AccountORM).filter(AccountORM.id.in_(ids)).all()
        return [self._to_domain(o) for o in orm_accounts]

    def count(self) -> int:
        return self.db.query(AccountORM).count()

    def add_interest(self, interest: AccountInterest) -> AccountInterest:
        orm = AccountInterestORM(account_id=interest.account_id, interest=interest.interest)
        self.db.add(orm)
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            existing = (
                self.db.query(AccountInterestORM)
                .filter(
                    AccountInterestORM.account_id == interest.account_id,
                    AccountInterestORM.interest == interest.interest,
                )
                .one_or_none()
            )
            if existing:
                return self._interest_to_domain(existing)
            raise
        self.db.refresh(orm)
        return self._interest_to_domain(orm)

    def delete_interest(self, account_id: int, interest_id: int) -> None:
        self.db.query(AccountInterestORM).filter(
            AccountInterestORM.account_id == account_id,
            AccountInterestORM.id == interest_id,
        ).delete()
        self.db.commit()

    def list_interests(self, account_id: int) -> List[AccountInterest]:
        interests = (
            self.db.query(AccountInterestORM)
            .filter(AccountInterestORM.account_id == account_id)
            .order_by(AccountInterestORM.created_at.asc())
            .all()
        )
        return [self._interest_to_domain(i) for i in interests]

    @staticmethod
    def _to_domain(orm_account: AccountORM) -> Account:
        account = Account(
            email=orm_account.email,
            nickname=orm_account.nickname,
            bio=getattr(orm_account, "bio", None),
            profile_image_url=getattr(orm_account, "profile_image_url", None),
        )
        account.id = orm_account.id
        account.created_at = orm_account.created_at
        account.updated_at = orm_account.updated_at
        return account

    @staticmethod
    def _interest_to_domain(orm: AccountInterestORM) -> AccountInterest:
        interest = AccountInterest(account_id=orm.account_id, interest=orm.interest)
        interest.id = orm.id
        interest.created_at = orm.created_at
        return interest
