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
    delete,
    func,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from threading import Lock

MAX_BALANCE = Decimal("999999999999999999.99")


class Base(DeclarativeBase):
    pass


class WitnessBase(DeclarativeBase):
    pass


class WitnessLeader(WitnessBase):
    __tablename__ = "bank_witness_leaders"

    cluster_id: Mapped[str] = mapped_column(String, primary_key=True)
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (CheckConstraint(
        "balance >= 0", name="accounts_balance_nonnegative"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    wallet: Mapped[str] = mapped_column(
        String, unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0"))
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
        CheckConstraint("entry IN ('in', 'out')",
                        name="bank_transactions_entry_valid"),
        CheckConstraint(
            "amount > 0", name="bank_transactions_amount_positive"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    request_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    account: Mapped[str] = mapped_column(
        ForeignKey("accounts.wallet"), nullable=False)
    entry: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    balance_before: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False)
    client_ip: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class BankOperation(Base):
    __tablename__ = "bank_operations"

    request_id: Mapped[str] = mapped_column(String, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class BankReplicaState(Base):
    __tablename__ = "bank_replica_state"

    cluster_id: Mapped[str] = mapped_column(String, primary_key=True)
    synchronized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


@dataclass(frozen=True)
class Operation:
    action: str
    account: str
    amount: Decimal = Decimal("0")
    destination: str | None = None

    def fingerprint(self) -> str:
        return json.dumps(
            {
                "action": self.action,
                "account": self.account,
                "amount": format(self.amount, "f"),
                "destination": self.destination,
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

    def export_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        with self.session_factory() as session:
            accounts = session.scalars(
                select(Account).order_by(Account.wallet)).all()
            transactions = session.scalars(
                select(BankTransaction).order_by(BankTransaction.id)
            ).all()
            operations = session.scalars(
                select(BankOperation).order_by(BankOperation.request_id)
            ).all()
            return {
                "accounts": [
                    {
                        "id": account.id,
                        "wallet": account.wallet,
                        "name": account.name,
                        "balance": str(account.balance),
                        "created_at": account.created_at.isoformat(),
                        "updated_at": account.updated_at.isoformat(),
                    }
                    for account in accounts
                ],
                "transactions": [
                    {
                        "request_id": transaction.request_id,
                        "action": transaction.action,
                        "account": transaction.account,
                        "entry": transaction.entry,
                        "amount": str(transaction.amount),
                        "balance_before": str(transaction.balance_before),
                        "balance_after": str(transaction.balance_after),
                        "client_ip": transaction.client_ip,
                        "created_at": transaction.created_at.isoformat(),
                    }
                    for transaction in transactions
                ],
                "operations": [
                    {
                        "request_id": operation.request_id,
                        "fingerprint": operation.fingerprint,
                        "response_json": operation.response_json,
                        "created_at": operation.created_at.isoformat(),
                    }
                    for operation in operations
                ],
            }

    def replica_epoch(self, cluster_id: str) -> int | None:
        with self.session_factory() as session:
            state = session.get(BankReplicaState, cluster_id)
            return state.epoch if state is not None else None

    def set_replica_epoch(self, cluster_id: str, epoch: int) -> None:
        with self.session_factory.begin() as session:
            session.execute(
                insert(BankReplicaState)
                .values(
                    cluster_id=cluster_id,
                    synchronized_at=datetime.now(timezone.utc),
                    epoch=epoch,
                )
                .on_conflict_do_update(
                    index_elements=[BankReplicaState.cluster_id],
                    set_={
                        "synchronized_at": datetime.now(timezone.utc),
                        "epoch": epoch,
                    },
                )
            )

    def restore_snapshot(
        self,
        snapshot: dict[str, Any],
        cluster_id: str,
        epoch: int,
    ) -> None:
        accounts = snapshot.get("accounts")
        transactions = snapshot.get("transactions")
        operations = snapshot.get("operations")
        if not all(isinstance(rows, list) for rows in (accounts, transactions, operations)):
            raise ValueError(
                "Snapshot must contain account, transaction, and operation lists.")
        if not all(
            isinstance(row, dict)
            for rows in (accounts, transactions, operations)
            for row in rows
        ):
            raise ValueError("Snapshot rows must be objects.")

        def timestamp(value: str) -> datetime:
            return datetime.fromisoformat(value)

        with self.session_factory.begin() as session:
            session.execute(delete(BankTransaction))
            session.execute(delete(BankOperation))
            session.execute(delete(Account))
            session.add_all(
                Account(
                    id=row["id"],
                    wallet=row["wallet"],
                    name=row["name"],
                    balance=Decimal(row["balance"]),
                    created_at=timestamp(row["created_at"]),
                    updated_at=timestamp(row["updated_at"]),
                )
                for row in accounts
            )
            session.add_all(
                BankTransaction(
                    request_id=row["request_id"],
                    action=row["action"],
                    account=row["account"],
                    entry=row["entry"],
                    amount=Decimal(row["amount"]),
                    balance_before=Decimal(row["balance_before"]),
                    balance_after=Decimal(row["balance_after"]),
                    client_ip=row["client_ip"],
                    created_at=timestamp(row["created_at"]),
                )
                for row in transactions
            )
            session.add_all(
                BankOperation(
                    request_id=row["request_id"],
                    fingerprint=row["fingerprint"],
                    response_json=row["response_json"],
                    created_at=timestamp(row["created_at"]),
                )
                for row in operations
            )
            session.flush()
            session.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('accounts', 'id'), "
                    "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM accounts"
                )
            )
            session.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('bank_transactions', 'id'), "
                    "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM bank_transactions"
                )
            )
            session.merge(
                BankReplicaState(
                    cluster_id=cluster_id,
                    synchronized_at=datetime.now(timezone.utc),
                    epoch=epoch,
                )
            )

    def execute(
        self,
        operation: Operation,
        request_id: str,
        client_ip: str,
        cluster_id: str | None = None,
        epoch: int | None = None,
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
                            func.concat(func.current_schema(),
                                        ":", request_id),
                            0,
                        )
                    )
                )
            )
            if cluster_id is not None and epoch is not None:
                session.execute(
                    insert(BankReplicaState)
                    .values(
                        cluster_id=cluster_id,
                        synchronized_at=datetime.now(timezone.utc),
                        epoch=epoch,
                    )
                    .on_conflict_do_update(
                        index_elements=[BankReplicaState.cluster_id],
                        set_={
                            "synchronized_at": datetime.now(timezone.utc),
                            "epoch": epoch,
                        },
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

            destination: Account | None = None
            if operation.action == "transfer":
                if operation.destination is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "missing_field",
                            "message": "destination is required for a transfer.",
                        },
                    )
                if operation.destination == operation.account:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_transfer",
                            "message": "Source and destination accounts must differ.",
                        },
                    )
                locked_accounts = session.scalars(
                    select(Account)
                    .where(
                        Account.wallet.in_(
                            [operation.account, operation.destination]
                        )
                    )
                    .order_by(Account.wallet)
                    .with_for_update()
                ).all()
                accounts = {
                    account.wallet: account for account in locked_accounts}
                source = accounts.get(operation.account)
                destination = accounts.get(operation.destination)
                if source is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.account} does not exist.",
                        },
                    )
                if destination is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.destination} does not exist.",
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
                if destination is None:
                    raise RuntimeError("Transfer destination was not locked.")
                if source.balance < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Insufficient funds.",
                        },
                    )
                source_before = source.balance
                destination_before = destination.balance
                source_after = source_before - operation.amount
                destination_after = destination_before + operation.amount
                if destination_after > MAX_BALANCE:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "Resulting destination balance exceeds the supported range.",
                        },
                    )
                self._set_balance(source, source_after)
                self._set_balance(destination, destination_after)
                self._add_transaction(
                    session,
                    operation,
                    source,
                    "out",
                    source_before,
                    source_after,
                    request_id,
                    client_ip,
                )
                self._add_transaction(
                    session,
                    operation,
                    destination,
                    "in",
                    destination_before,
                    destination_after,
                    request_id,
                    client_ip,
                )
                response = {
                    "status": "success",
                    "source_account": operation.account,
                    "source_balance": float(source_after),
                    "destination": operation.destination,
                    "destination_balance": float(destination_after),
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


class BankWitness:
    def __init__(self, database_url: str, cluster_id: str, logger: Any) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError(
                "BANK_WITNESS_DATABASE_URL must use the PostgreSQL protocol.")
        self.cluster_id = cluster_id
        self.logger = logger
        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 3,
                "options": "-c synchronous_commit=on -c statement_timeout=3000",
            },
        )
        self._connection: Connection | None = None
        self._backend_pid: int | None = None
        self._epoch: int | None = None
        self._lock = Lock()

    def initialize(self) -> None:
        WitnessBase.metadata.create_all(self.engine)

    def _release_candidate(self, connection: Connection) -> None:
        try:
            connection.rollback()
            released = connection.scalar(
                select(
                    func.pg_advisory_unlock(
                        func.hashtextextended(self.cluster_id, 0)
                    )
                )
            )
            connection.commit()
            if not released:
                self.logger.error(
                    "PostgreSQL witness candidate lock was not held.")
        except SQLAlchemyError:
            self.logger.warning(
                "Could not release PostgreSQL witness candidate lock; closing its connection.",
                exc_info=True,
            )
            connection.invalidate()
        finally:
            connection.close()

    def try_acquire(
        self,
        node_id: str,
        local_epoch: int | None,
        allow_initial: bool = False,
    ) -> int | None:
        with self._lock:
            if self._connection is not None:
                if self._owns_lock():
                    return self._epoch
                self._close_connection()

            connection: Connection | None = None
            lock_acquired = False
            try:
                connection = self.engine.connect()
                acquired, backend_pid = connection.execute(
                    select(
                        func.pg_try_advisory_lock(
                            func.hashtextextended(self.cluster_id, 0)
                        ),
                        func.pg_backend_pid(),
                    )
                ).one()
                if not acquired:
                    connection.rollback()
                    connection.close()
                    return None
                lock_acquired = True
                previous = connection.execute(
                    select(WitnessLeader.node_id, WitnessLeader.epoch).where(
                        WitnessLeader.cluster_id == self.cluster_id
                    )
                ).first()
                if previous is None:
                    if not allow_initial:
                        self._release_candidate(connection)
                        return None
                    epoch = 1
                    connection.execute(
                        WitnessLeader.__table__.insert().values(
                            cluster_id=self.cluster_id,
                            node_id=node_id,
                            epoch=epoch,
                        )
                    )
                else:
                    previous_node, previous_epoch = previous
                    local_is_current = (
                        local_epoch is not None and local_epoch >= previous_epoch
                    )
                    recovering_own_promotion = (
                        previous_node == node_id
                        and local_epoch == previous_epoch - 1
                    )
                    if not local_is_current and not recovering_own_promotion:
                        self._release_candidate(connection)
                        return None
                    epoch = previous_epoch + 1
                    connection.execute(
                        update(WitnessLeader)
                        .where(WitnessLeader.cluster_id == self.cluster_id)
                        .values(node_id=node_id, epoch=epoch)
                    )
                connection.commit()
                self._connection = connection
                self._backend_pid = backend_pid
                self._epoch = epoch
                lock_acquired = False
                return epoch
            except SQLAlchemyError:
                if connection is not None:
                    if lock_acquired:
                        self._release_candidate(connection)
                    else:
                        connection.close()
                raise

    def is_current_leader(self, node_id: str, epoch: int) -> bool:
        leader = self.current_leader()
        return leader == (node_id, epoch)

    def current_leader(self) -> tuple[str, int] | None:
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    select(WitnessLeader.node_id, WitnessLeader.epoch).where(
                        WitnessLeader.cluster_id == self.cluster_id
                    )
                ).first()
            if result is None:
                return None
            return str(result[0]), int(result[1])
        except SQLAlchemyError:
            self.logger.warning(
                "Could not verify witness leader term.", exc_info=True)
            return None

    def owns_lock(self) -> bool:
        with self._lock:
            return self._owns_lock()

    def _owns_lock(self) -> bool:
        if self._connection is None or self._backend_pid is None:
            return False
        try:
            backend_pid = self._connection.scalar(
                select(func.pg_backend_pid()))
            self._connection.commit()
        except SQLAlchemyError:
            self.logger.warning(
                "Could not verify PostgreSQL witness lock.", exc_info=True)
            self._close_connection()
            return False
        if backend_pid != self._backend_pid:
            self._close_connection()
            return False
        return True

    def _close_connection(self) -> None:
        connection, self._connection = self._connection, None
        self._backend_pid = None
        self._epoch = None
        if connection is not None:
            try:
                connection.invalidate()
            except SQLAlchemyError:
                self.logger.warning(
                    "Could not invalidate the PostgreSQL witness connection.",
                    exc_info=True,
                )
            finally:
                try:
                    connection.close()
                except SQLAlchemyError:
                    self.logger.warning(
                        "Could not close the PostgreSQL witness connection.",
                        exc_info=True,
                    )

    def close(self) -> None:
        with self._lock:
            self._close_connection()
        self.engine.dispose()
