from __future__ import annotations

import unittest
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, MetaData, Numeric, String, Table, create_engine

from pyeasymatrixdb import DbDriver
from pyeasymatrixdb.subclasses.DbDriverUtils import DbDriverUtils


class RepeatedFilterColumnTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        self.metadata = MetaData()
        self.users = Table(
            "users",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        self.orders = Table(
            "orders",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", ForeignKey("users.id")),
            Column("status", String(20)),
            Column("product", String(50)),
            Column("used_on", Date),
            Column("delivered_at", DateTime),
            Column("amount", Numeric(10, 2)),
        )
        self.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.connection.execute(
            self.users.insert(),
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Carol"},
            ],
        )
        self.connection.execute(
            self.orders.insert(),
            [
                {
                    "id": 10,
                    "user_id": 1,
                    "status": "OPEN",
                    "product": "Cable 2x15",
                    "used_on": None,
                    "delivered_at": datetime(2026, 1, 2, 10, 0),
                    "amount": Decimal("10.50"),
                },
                {
                    "id": 20,
                    "user_id": 1,
                    "status": "CLOSED",
                    "product": "Cable 3x20",
                    "used_on": date(2026, 1, 20),
                    "delivered_at": None,
                    "amount": Decimal("20.00"),
                },
                {
                    "id": 30,
                    "user_id": 2,
                    "status": "OPEN",
                    "product": "Pipe 2x15",
                    "used_on": date(2026, 1, 30),
                    "delivered_at": datetime(2026, 1, 30, 10, 0),
                    "amount": Decimal("30.25"),
                },
            ],
        )
        self.driver = DbDriver(self.metadata, self.connection)

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()

    def search_order_ids(self, filters: list[list[object]], relationships: list[list[object]] | None = None, approximate: bool = False) -> list[int]:
        search = self.driver.Pesquisar.define_header([["orders"], ["id"]])
        if relationships:
            search = search.define_relationships(relationships)
        rows = search.define_filter(filters).search(reset=True, approximate=approximate)
        return [row[0] for row in rows[2:]]

    def compiled_sql(self, filters: list[list[object]], relationships: list[list[object]] | None = None) -> str:
        stmt = DbDriverUtils.buid_select(
            columns_definitions=self.driver.get_schema(),
            headers=[["orders"], ["id"]],
            relationships=relationships or [],
            filters=filters,
        )
        return str(stmt.compile(dialect=self.engine.dialect, compile_kwargs={"literal_binds": True}))

    def test_repeated_single_column_uses_or(self):
        filters = [
            ["orders", "orders"],
            ["id", "id"],
            ["10", "20"],
        ]

        self.assertEqual(self.search_order_ids(filters), [10, 20])
        self.assertNotIn(30, self.search_order_ids(filters))
        self.assertIn("orders.id = 10 OR orders.id = 20", self.compiled_sql(filters))

    def test_same_column_name_on_different_tables_is_grouped_separately(self):
        relationships = [["orders", "users", "user_id", "id", 1]]
        filters = [
            ["users", "users", "orders", "orders"],
            ["id", "id", "id", "id"],
            ["1", "2", "10", "30"],
        ]

        self.assertEqual(self.search_order_ids(filters, relationships), [10, 30])
        sql = self.compiled_sql(filters, relationships)
        self.assertIn("users.id = 1 OR users.id = 2", sql)
        self.assertIn("orders.id = 10 OR orders.id = 30", sql)

    def test_different_columns_in_same_row_still_use_and(self):
        filters = [
            ["orders", "orders"],
            ["id", "status"],
            ["10", "OPEN"],
        ]

        self.assertEqual(self.search_order_ids(filters), [10])
        self.assertIn("orders.id = 10 AND orders.status = 'OPEN'", self.compiled_sql(filters))

    def test_repeated_column_with_common_filter(self):
        filters = [
            ["orders", "orders", "orders"],
            ["id", "id", "status"],
            ["10", "20", "OPEN"],
        ]

        self.assertEqual(self.search_order_ids(filters), [10])
        self.assertIn("(orders.id = 10 OR orders.id = 20) AND orders.status = 'OPEN'", self.compiled_sql(filters))

    def test_multiple_rows_remain_or_between_complete_alternatives(self):
        filters = [
            ["orders", "orders"],
            ["id", "status"],
            ["10", "CLOSED"],
            ["30", "OPEN"],
        ]

        self.assertEqual(self.search_order_ids(filters), [30])

    def test_repeated_null_filters_keep_sql_null_semantics(self):
        self.assertEqual(self.search_order_ids([["orders"], ["used_on"], ["null"]]), [10])
        self.assertEqual(self.search_order_ids([["orders"], ["used_on"], ["=null"]]), [10])
        self.assertEqual(self.search_order_ids([["orders"], ["used_on"], ["==null"]]), [10])
        self.assertEqual(self.search_order_ids([["orders"], ["used_on"], ["!=null"]]), [20, 30])

        filters = [
            ["orders", "orders"],
            ["delivered_at", "delivered_at"],
            ["!=null", "!= NULL"],
        ]
        sql = self.compiled_sql(filters)
        self.assertEqual(self.search_order_ids(filters), [10, 30])
        self.assertIn("IS NOT NULL", sql)
        self.assertNotIn("!= 'null'", sql.lower())
        self.assertNotIn("= 'null'", sql.lower())

    def test_repeated_filters_work_for_supported_types(self):
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["id", "id"], ["10", "20"]]), [10, 20])
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["status", "status"], ["OPEN", "CLOSED"]]), [10, 20, 30])
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["used_on", "used_on"], ["2026-01-20", "2026-01-30"]]), [20, 30])
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["delivered_at", "delivered_at"], ["2026-01-02 10:00:00", "2026-01-30 10:00:00"]]), [10, 30])
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["amount", "amount"], ["10.50", "30.25"]]), [10, 30])

    def test_regression_operators_wildcard_and_non_duplicate_filters(self):
        self.assertEqual(self.search_order_ids([["orders"], ["id"], [">10"]]), [20, 30])
        self.assertEqual(self.search_order_ids([["orders"], ["id"], [">=20"]]), [20, 30])
        self.assertEqual(self.search_order_ids([["orders"], ["id"], ["<20"]]), [10])
        self.assertEqual(self.search_order_ids([["orders"], ["id"], ["<=20"]]), [10, 20])
        self.assertEqual(self.search_order_ids([["orders"], ["id"], ["!=20"]]), [10, 30])
        self.assertEqual(self.search_order_ids([["orders"], ["id"], ["==20"]]), [20])
        self.assertEqual(self.search_order_ids([["orders"], ["product"], ["*2x15"]]), [10, 30])
        self.assertEqual(self.search_order_ids([["orders", "orders"], ["id", "status"], ["10", "OPEN"]]), [10])

    def test_approximate_true_still_wraps_text_filters(self):
        filters = [["orders"], ["product"], ["Pipe"]]

        self.assertEqual(self.search_order_ids(filters, approximate=True), [30])

    def test_filters_without_valid_conditions_return_unfiltered_rows(self):
        filters = [["orders"], ["id"], [""]]

        self.assertEqual(self.search_order_ids(filters), [10, 20, 30])

    def test_repeated_columns_do_not_create_implicit_pairs(self):
        filters = [
            ["orders", "orders", "orders", "orders"],
            ["id", "id", "product", "product"],
            ["10", "20", "*2x15", "*3x20"],
        ]

        self.assertEqual(self.search_order_ids(filters), [10, 20])


if __name__ == "__main__":
    unittest.main()