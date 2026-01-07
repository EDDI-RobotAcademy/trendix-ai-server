from typing import Optional, List
from abc import ABC, abstractmethod
from account.domain.account import Account
from account.domain.interest import AccountInterest
from account.domain.dashboard_layout import DashboardLayout

class AccountRepositoryPort(ABC):

    @abstractmethod
    def save(self, account: Account) -> Account:
        pass

    @abstractmethod
    def update(self, account: Account) -> Account:
        pass

    @abstractmethod
    def find_by_id(self, account_id: int) -> Optional[Account]:
        pass

    @abstractmethod
    def find_by_email(self, email: str) -> Optional[Account]:
        pass

    @abstractmethod
    def find_all_by_id(self, ids: list[int]) -> List[Account]:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def add_interest(self, interest: AccountInterest) -> AccountInterest:
        pass

    @abstractmethod
    def delete_interest(self, account_id: int, interest_id: int) -> None:
        pass

    @abstractmethod
    def list_interests(self, account_id: int) -> List[AccountInterest]:
        pass

    @abstractmethod
    def save_dashboard_layout(self, layout: DashboardLayout) -> DashboardLayout:
        """대시보드 레이아웃 저장 (없으면 생성, 있으면 업데이트)"""
        pass

    @abstractmethod
    def get_dashboard_layout(self, account_id: int) -> Optional[DashboardLayout]:
        """대시보드 레이아웃 조회"""
        pass
