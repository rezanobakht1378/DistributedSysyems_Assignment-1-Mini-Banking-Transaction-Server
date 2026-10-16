from datetime import datetime
from enum import Enum
from typing import Optional
from sqlmodel import SQLModel, Field, Column
import sqlalchemy as sa

class TransactionType(str, Enum):
    deposit = "deposit"
    withdraw = "withdraw"
    transfer = "transfer"

class Account(SQLModel, table=True):
    __tablename__ = "accounts"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet: str = Field(index=True, unique=True, nullable=False)
    name: str = Field(nullable=False)
    balance: float = Field(default=0.0, nullable=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow, sa_column_kwargs={"onupdate": datetime.utcnow})

class Transaction(SQLModel, table=True):
    __tablename__ = "transactions"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_id: str = Field(index=True, nullable=False)
    amount: float = Field(nullable=False)
    last_balance: float = Field(nullable=False)
    new_balance: float = Field(nullable=False)
    type: TransactionType = Field(nullable=False)
    ip: str = Field(nullable=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)