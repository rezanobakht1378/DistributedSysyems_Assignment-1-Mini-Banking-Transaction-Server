import datetime
from pydantic import BaseModel, IPADDRESS
from enum import Enum

class Account(BaseModel):
    name: str
    wallet: str
    balance: float
    created_at: datetime
    updated_at: datetime

class Transaction(BaseModel):
    wallet_id: str
    amount: float
    last_balance: float
    new_baloance: float
    type: TransactionType
    ip: Ip
    created_at: datetime
    updated_at: datetime

class TransactionType(str, Enum):
    deposit = "deposit"
    withdraw = "withdraw"
    transfer = "transfer"