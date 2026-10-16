from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Text,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

MAX_BALANCE = Decimal("999999999999999999.99")


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (CheckConstraint("balance >= 0", name="accounts_balance_nonnegative"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False, default=Decimal("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class BankTransaction(Base):
    __tablename__ = "bank_transactions"
    __table_args__ = (
        CheckConstraint("entry IN ('in', 'out')", name="bank_transactions_entry_valid"),
        CheckConstraint("amount > 0", name="bank_transactions_amount_positive"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    request_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.wallet"), nullable=False)
    entry: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    balance_before: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    client_ip: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class BankOperation(Base):
    __tablename__ = "bank_operations"

    request_id: Mapped[str] = mapped_column(String, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Operation:
    action: str
    account: str
    amount: Decimal = Decimal("0")
    target_account: str | None = None

    def fingerprint(self) -> str:
        return json.dumps(
            {
                "action": self.action,
                "account": self.account,
                "amount": format(self.amount, "f"),
                "target_account": self.target_account,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


class BankStore:
    def __init__(self, database_url: str) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must use the PostgreSQL protocol.")
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={"options": "-c synchronous_commit=on"},
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=Session,
            expire_on_commit=False,
        )

    def initialize(self) -> None:
        Base.metadata.create_all(self.engine)
        now = datetime.now(timezone.utc)
        accounts = [
            {
                "wallet": "A1001",
                "name": "Sample Customer 1",
                "balance": Decimal("1000.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A1002",
                "name": "Sample Customer 2",
                "balance": Decimal("500.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A1003",
                "name": "Sample Customer 3",
                "balance": Decimal("2000.00"),
                "created_at": now,
                "updated_at": now,
            },
        ]
        with self.session_factory.begin() as session:
            session.execute(
                insert(Account)
                .values(accounts)
                .on_conflict_do_nothing(index_elements=[Account.wallet])
            )

    def get_balance(self, account: str) -> dict[str, Any]:
        with self.session_factory() as session:
            record = session.scalar(
                select(Account).where(Account.wallet == account)
            )
            if record is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error": "unknown_account",
                        "message": f"Account {account} does not exist.",
                    },
                )
            return {
                "status": "success",
                "account": account,
                "balance": float(record.balance),
            }

    def execute(
        self,
        operation: Operation,
        request_id: str,
        client_ip: str,
    ) -> dict[str, Any]:
        with self.session_factory.begin() as session:
            if operation.amount <= 0 or operation.amount > MAX_BALANCE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_amount",
                        "message": "amount is outside the supported range.",
                    },
                )

            session.scalar(
                select(
                    func.pg_advisory_xact_lock(
                        func.hashtextextended(
                            func.concat(func.current_schema(), ":", request_id),
                            0,
                        )
                    )
                )
            )
            existing = session.get(BankOperation, request_id)
            fingerprint = operation.fingerprint()
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "request_id_conflict",
                            "message": "This request ID was already used for a different operation.",
                        },
                    )
                return json.loads(existing.response_json)

            if operation.action == "transfer":
                if operation.target_account is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "missing_field",
                            "message": "target_account is required for a transfer.",
                        },
                    )
                if operation.target_account == operation.account:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_transfer",
                            "message": "Source and target accounts must differ.",
                        },
                    )

                locked_accounts = session.scalars(
                    select(Account)
                    .where(Account.wallet.in_([operation.account, operation.target_account]))
                    .order_by(Account.wallet)
                    .with_for_update()
                ).all()
                accounts = {record.wallet: record for record in locked_accounts}
                source = accounts.get(operation.account)
                target = accounts.get(operation.target_account)
                if source is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.account} does not exist.",
                        },
                    )
                if target is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.target_account} does not exist.",
                        },
                    )
            else:
                source = session.scalar(
                    select(Account)
                    .where(Account.wallet == operation.account)
                    .with_for_update()
                )
                if source is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.account} does not exist.",
                        },
                    )

            if operation.action == "deposit":
                previous_balance = source.balance
                new_balance = previous_balance + operation.amount
                if new_balance > MAX_BALANCE:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "resulting balance exceeds the supported range.",
                        },
                    )
                self._set_balance(source, new_balance)
                self._add_transaction(
                    session, operation, source, "in", previous_balance,
                    new_balance, request_id, client_ip,
                )
                response: dict[str, Any] = {
                    "status": "success",
                    "account": operation.account,
                    "balance": float(new_balance),
                }
            elif operation.action == "withdraw":
                if source.balance < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Insufficient funds.",
                        },
                    )
                previous_balance = source.balance
                new_balance = previous_balance - operation.amount
                self._set_balance(source, new_balance)
                self._add_transaction(
                    session, operation, source, "out", previous_balance,
                    new_balance, request_id, client_ip,
                )
                response = {
                    "status": "success",
                    "account": operation.account,
                    "balance": float(new_balance),
                }
            elif operation.action == "transfer":
                if source.balance < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Insufficient funds.",
                        },
                    )
                source_before = source.balance
                target_before = target.balance
                source_after = source_before - operation.amount
                target_after = target_before + operation.amount
                if target_after > MAX_BALANCE:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "resulting balance exceeds the supported range.",
                        },
                    )
                self._set_balance(source, source_after)
                self._set_balance(target, target_after)
                self._add_transaction(
                    session, operation, source, "out", source_before,
                    source_after, request_id, client_ip,
                )
                self._add_transaction(
                    session, operation, target, "in", target_before,
                    target_after, request_id, client_ip,
                )
                response = {
                    "status": "success",
                    "source_account": operation.account,
                    "source_balance": float(source_after),
                    "target_account": operation.target_account,
                    "target_balance": float(target_after),
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "unsupported_action",
                        "message": f"Action {operation.action!r} is not supported.",
                    },
                )

            session.add(
                BankOperation(
                    request_id=request_id,
                    fingerprint=fingerprint,
                    response_json=json.dumps(response, separators=(",", ":")),
                )
            )
            return response

    @staticmethod
    def _set_balance(account: Account, amount: Decimal) -> None:
        account.balance = amount
        account.updated_at = datetime.now(timezone.utc)

    @staticmethod
    def _add_transaction(
        session: Session,
        operation: Operation,
        account: Account,
        entry: str,
        balance_before: Decimal,
        balance_after: Decimal,
        request_id: str,
        client_ip: str,
    ) -> None:
        session.add(
            BankTransaction(
                request_id=request_id,
                action=operation.action,
                account=account.wallet,
                entry=entry,
                amount=operation.amount,
                balance_before=balance_before,
                balance_after=balance_after,
                client_ip=client_ip,
                created_at=datetime.now(timezone.utc),
            )
        )
