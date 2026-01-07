from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InvalidRequestError, PendingRollbackError, OperationalError
from sqlalchemy.orm import Session
from account.application.port.account_repository_port import AccountRepositoryPort
from account.domain.account import Account
from account.domain.interest import AccountInterest
from account.domain.dashboard_layout import DashboardLayout
from account.infrastructure.orm.account_orm import AccountORM, AccountInterestORM, AccountDashboardLayoutORM
from config.database.session import get_db_session


class AccountRepositoryImpl(AccountRepositoryPort):
    def __init__(self):
        self.db: Session = get_db_session()

    def _ensure_session(self):
        """세션이 유효하지 않으면 재생성"""
        try:
            # 세션 상태 확인을 위해 간단한 쿼리 시도
            self.db.execute(text("SELECT 1"))
        except (InvalidRequestError, PendingRollbackError, OperationalError):
            # 세션이 invalid 상태거나 연결이 끊어진 경우, 새 세션 생성
            # (기존 세션 정리는 GC가 처리)
            self.db = get_db_session()

    def save(self, account: Account) -> Account:
        self._ensure_session()
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
        self._ensure_session()
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
        self._ensure_session()
        orm_account = self.db.get(AccountORM, account_id)
        if orm_account is None:
            return None
        return self._to_domain(orm_account)

    def find_by_email(self, email: str) -> Account | None:
        self._ensure_session()
        orm_account = self.db.query(AccountORM).filter(AccountORM.email == email).first()
        if orm_account is None:
            return None
        return self._to_domain(orm_account)

    def find_all_by_id(self, ids: list[int]) -> List[Account]:
        self._ensure_session()
        orm_accounts = self.db.query(AccountORM).filter(AccountORM.id.in_(ids)).all()
        return [self._to_domain(o) for o in orm_accounts]

    def count(self) -> int:
        self._ensure_session()
        return self.db.query(AccountORM).count()

    def add_interest(self, interest: AccountInterest) -> AccountInterest:
        self._ensure_session()
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
        self._ensure_session()
        self.db.query(AccountInterestORM).filter(
            AccountInterestORM.account_id == account_id,
            AccountInterestORM.id == interest_id,
        ).delete()
        self.db.commit()

    def list_interests(self, account_id: int) -> List[AccountInterest]:
        self._ensure_session()
        interests = (
            self.db.query(AccountInterestORM)
            .filter(AccountInterestORM.account_id == account_id)
            .order_by(AccountInterestORM.created_at.asc())
            .all()
        )
        return [self._interest_to_domain(i) for i in interests]

    def save_dashboard_layout(self, layout: DashboardLayout) -> DashboardLayout:
        """대시보드 레이아웃 저장 (없으면 생성, 있으면 업데이트)"""
        self._ensure_session()

        # 기존 레이아웃 조회
        existing = (
            self.db.query(AccountDashboardLayoutORM)
            .filter(AccountDashboardLayoutORM.account_id == layout.account_id)
            .one_or_none()
        )

        if existing:
            # 업데이트
            existing.widgets = layout.widgets
            existing.layouts = layout.layouts
            self.db.commit()
            self.db.refresh(existing)
            return self._dashboard_layout_to_domain(existing)
        else:
            # 생성
            orm = AccountDashboardLayoutORM(
                account_id=layout.account_id,
                widgets=layout.widgets,
                layouts=layout.layouts,
            )
            self.db.add(orm)
            self.db.commit()
            self.db.refresh(orm)
            return self._dashboard_layout_to_domain(orm)

    def get_dashboard_layout(self, account_id: int) -> Optional[DashboardLayout]:
        """대시보드 레이아웃 조회"""
        self._ensure_session()
        orm = (
            self.db.query(AccountDashboardLayoutORM)
            .filter(AccountDashboardLayoutORM.account_id == account_id)
            .one_or_none()
        )
        if orm is None:
            return None
        return self._dashboard_layout_to_domain(orm)

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

    @staticmethod
    def _dashboard_layout_to_domain(orm: AccountDashboardLayoutORM) -> DashboardLayout:
        layout = DashboardLayout(
            account_id=orm.account_id,
            widgets=orm.widgets,
            layouts=orm.layouts,
        )
        layout.id = orm.id
        layout.created_at = orm.created_at
        layout.updated_at = orm.updated_at
        return layout
