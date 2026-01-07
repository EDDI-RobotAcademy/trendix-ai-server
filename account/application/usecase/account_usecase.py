from typing import List, Optional, Any

from account.application.port.account_repository_port import AccountRepositoryPort
from account.domain.account import Account
from account.domain.interest import AccountInterest
from account.domain.dashboard_layout import DashboardLayout


class AccountUseCase:
    def __init__(self, account_repository: AccountRepositoryPort):
        self.repo = account_repository

    def create_or_get_account(self, email: str, nickname: str | None):
        account = self.repo.find_by_email(email)
        if account:
            return account

        if not nickname:
            total = self.repo.count()
            nickname = f"anonymous{total + 1}"

        account = Account(email=email, nickname=nickname)
        return self.repo.save(account)

    def get_account_by_id(self, account_id: int) -> Optional[Account]:
        accounts = self.get_accounts_by_ids([account_id])
        return accounts[0] if accounts else None

    def get_accounts_by_ids(self, ids: list[int]) -> List[Account]:

        if not ids:
            return []

        return self.repo.find_all_by_id(ids)

    def update_profile(
        self,
        account_id: int,
        nickname: Optional[str] = None,
        bio: Optional[str] = None,
        profile_image_url: Optional[str] = None,
    ) -> Account:
        account = self.repo.find_by_id(account_id)
        if account is None:
            raise ValueError("Account not found")

        account.update_profile(nickname=nickname, bio=bio, profile_image_url=profile_image_url)
        return self.repo.update(account)

    def list_interests(self, account_id: int) -> List[AccountInterest]:
        if self.repo.find_by_id(account_id) is None:
            raise ValueError("Account not found")
        return self.repo.list_interests(account_id)

    def add_interest(self, account_id: int, interest: str) -> AccountInterest:
        if self.repo.find_by_id(account_id) is None:
            raise ValueError("Account not found")
        interest_model = AccountInterest(account_id=account_id, interest=interest)
        return self.repo.add_interest(interest_model)

    def delete_interest(self, account_id: int, interest_id: int) -> None:
        if self.repo.find_by_id(account_id) is None:
            raise ValueError("Account not found")
        self.repo.delete_interest(account_id, interest_id)

    def save_dashboard_layout(self, account_id: int, widgets: Any, layouts: Any) -> DashboardLayout:
        """대시보드 레이아웃 저장"""
        if self.repo.find_by_id(account_id) is None:
            raise ValueError("Account not found")
        layout = DashboardLayout(account_id=account_id, widgets=widgets, layouts=layouts)
        return self.repo.save_dashboard_layout(layout)

    def get_dashboard_layout(self, account_id: int) -> Optional[DashboardLayout]:
        """대시보드 레이아웃 조회"""
        if self.repo.find_by_id(account_id) is None:
            raise ValueError("Account not found")
        return self.repo.get_dashboard_layout(account_id)
