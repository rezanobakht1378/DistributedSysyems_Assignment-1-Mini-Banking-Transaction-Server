from decimal import Decimal
import logging
import os
import unittest
import uuid

from fastapi import HTTPException
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn

from models import BankStore, BankWitness, Operation


class SplitBrainWitnessTests(unittest.TestCase):
    def setUp(self) -> None:
        database_url = os.getenv("BANK_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest(
                "Set BANK_TEST_DATABASE_URL to a dedicated PostgreSQL test database."
            )

        self.database_url = database_url
        self.schemas: list[str] = []
        self.addCleanup(self._drop_schemas)
        account_a_url = self._create_schema_url()
        account_b_url = self._create_schema_url()
        self.witness_url = self._create_schema_url()
        self.store_a = BankStore(account_a_url)
        self.store_b = BankStore(account_b_url)
        self.witness_a = BankWitness(
            self.witness_url,
            "split-brain-test",
            logging.getLogger("split_brain_test"),
        )
        self.witness_b = BankWitness(
            self.witness_url,
            "split-brain-test",
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(self.store_a.engine.dispose)
        self.addCleanup(self.store_b.engine.dispose)
        self.addCleanup(self.witness_a.close)
        self.addCleanup(self.witness_b.close)

        self.store_a.initialize()
        self.store_b.initialize()
        self.witness_a.initialize()

    def _create_schema_url(self) -> str:
        schema_name = f"bank_test_{uuid.uuid4().hex}"
        connection = psycopg2.connect(self.database_url)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
                )
        finally:
            connection.close()
        self.schemas.append(schema_name)

        parameters = parse_dsn(self.database_url)
        parameters["options"] = f"-c search_path={schema_name}"
        return make_dsn(**parameters)

    def _drop_schemas(self) -> None:
        connection = psycopg2.connect(self.database_url)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for schema_name in self.schemas:
                    cursor.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                            sql.Identifier(schema_name)
                        )
                    )
        finally:
            connection.close()

    def test_partition_blocks_promotion_and_rejoining_primary_catches_up(self) -> None:
        cluster_id = "split-brain-test"
        first_epoch = self.witness_a.try_acquire(
            "server-a",
            self.store_a.replica_epoch(cluster_id),
            allow_initial=True,
        )
        self.assertEqual(first_epoch, 1)
        self.store_a.set_replica_epoch(cluster_id, first_epoch)
        first_result = self.store_a.execute(
            Operation("deposit", "A1001", Decimal("5.00")),
            "confirmed-deposit",
            "127.0.0.1",
            cluster_id,
            first_epoch,
        )
        self.store_b.restore_snapshot(
            self.store_a.export_snapshot(),
            cluster_id,
            first_epoch,
        )
        duplicate_result = self.store_b.execute(
            Operation("deposit", "A1001", Decimal("5.00")),
            "confirmed-deposit",
            "127.0.0.1",
            cluster_id,
            first_epoch,
        )
        self.assertEqual(duplicate_result, first_result)
        self.assertEqual(self.store_b.get_balance("A1001")["balance"], 1005.0)

        self.assertIsNone(
            self.witness_b.try_acquire(
                "server-b",
                self.store_b.replica_epoch(cluster_id),
            )
        )

        self.witness_a.close()
        second_epoch = self.witness_b.try_acquire(
            "server-b",
            self.store_b.replica_epoch(cluster_id),
        )
        self.assertEqual(second_epoch, 2)
        self.store_b.set_replica_epoch(cluster_id, second_epoch)
        self.witness_a = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(self.witness_a.close)

        self.assertIsNone(
            self.witness_a.try_acquire(
                "server-a",
                self.store_a.replica_epoch(cluster_id),
            )
        )
        self.witness_b.close()
        self.assertIsNone(
            self.witness_a.try_acquire("server-a", first_epoch)
        )

        self.store_a.restore_snapshot(
            self.store_b.export_snapshot(),
            cluster_id,
            second_epoch,
        )
        third_epoch = self.witness_a.try_acquire(
            "server-a",
            self.store_a.replica_epoch(cluster_id),
        )
        self.assertEqual(third_epoch, 3)
        self.assertEqual(
            self.store_a.get_balance("A1001")["balance"],
            1005.0,
        )

    def test_rejected_stale_candidate_releases_witness_lock(self) -> None:
        cluster_id = "stale-lock-test"
        first_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        candidate_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(first_witness.close)
        self.addCleanup(candidate_witness.close)
        first_epoch = first_witness.try_acquire(
            "server-a",
            None,
            allow_initial=True,
        )
        self.assertEqual(first_epoch, 1)
        first_witness.close()

        self.assertIsNone(candidate_witness.try_acquire("server-b", None))

        recovery_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(recovery_witness.close)
        self.assertEqual(recovery_witness.try_acquire("server-a", 1), 2)

    def test_transfer_commits_both_accounts_and_is_idempotent(self) -> None:
        operation = Operation(
            "transfer",
            "A1001",
            Decimal("25.00"),
            "A1002",
        )
        result = self.store_a.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )
        replay = self.store_a.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )
        replica_result = self.store_b.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )

        self.assertEqual(result, replay)
        self.assertEqual(result, replica_result)
        self.assertEqual(self.store_a.get_balance("A1001")["balance"], 975.0)
        self.assertEqual(self.store_a.get_balance("A1002")["balance"], 525.0)
        self.assertEqual(self.store_b.get_balance("A1001")["balance"], 975.0)
        self.assertEqual(self.store_b.get_balance("A1002")["balance"], 525.0)

    def test_failed_transfer_leaves_both_accounts_unchanged(self) -> None:
        before_source = self.store_a.get_balance("A1001")["balance"]
        before_destination = self.store_a.get_balance("A1002")["balance"]
        with self.assertRaises(HTTPException):
            self.store_a.execute(
                Operation(
                    "transfer",
                    "A1001",
                    Decimal("5000.00"),
                    "A1002",
                ),
                "failed-transfer",
                "127.0.0.1",
            )

        self.assertEqual(
            self.store_a.get_balance("A1001")["balance"],
            before_source,
        )
        self.assertEqual(
            self.store_a.get_balance("A1002")["balance"],
            before_destination,
        )


if __name__ == "__main__":
    unittest.main()
