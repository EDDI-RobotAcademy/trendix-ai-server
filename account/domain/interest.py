from datetime import datetime
from typing import Optional


class AccountInterest:
    def __init__(self, account_id: int, interest: str):
        self.id: Optional[int] = None
        self.account_id = account_id
        self.interest = interest
        self.created_at: datetime = datetime.utcnow()
