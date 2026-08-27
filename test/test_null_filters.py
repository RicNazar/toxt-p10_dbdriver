from __future__ import annotations

import unittest
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, MetaData, Numeric, String, Table, create_engine

from pyeasymatrixdb import DbDriver


class NullFilterTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        self.metadata = MetaData()
        self.items = Table(
            "items",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("qty", Integer),
            Column("name", String(50)),
            Column("created_on", Date),
            Column("updated_at", DateTime),
            Column("active", Boolean),
            Column("amount", Numeric(10, 2)),
        )
        self.metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.connection.execute(
            self.items.insert(),
            [
                {
                    "id": 1,
                    "qty": None,
                    "name": None,
                    "created_on": None,
                    "updated_at": None,
                    "active": None,
                    "amount": None,
                },
                {
                    "id": 2,
                    "qty": 10,
                    "name": "null",
                    "created_on": date(2026, 1, 2),
                    "updated_at": datetime(2026, 1, 2, 3, 4, 5),
                    "active": True,
                    "amount": Decimal("10.50"),
                },
                {
                    "id": 3,
                    "qty": 20,
                    "name": "alpha",
                    "created_on": date(2026, 2, 3),
                    "updated_at": datetime(2026, 2, 3, 4, 5, 6),
                    "active": False,
                    "amount": Decimal("20.75"),
                },
            ],
        )
        self.driver = DbDriver(self.metadata, self.connection)

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()

    def search_ids(self, column_name: str, raw_value: object) -> list[int]:
        rows = (
            self.driver.Pesquisar
            .define_header([["items"], ["id"]])
            .define_filter([["items"], [column_name], [raw_value]])
            .search(reset=True)
        )
        return [row[0] for row in rows[2:]]

    def statement_sql(self, column_name: str, raw_value: object) -> str:
        rows = (
            self.driver.Pesquisar
            .define_header([["items"], ["id"]])
            .define_filter([["items"], [column_name], [raw_value]])
            .search(reset=True, only_stmt=True)
        )
        return rows[0][0]

    def test_integer_null_filters(self):
        self.assertEqual(self.search_ids("qty", "null"), [1])
        self.assertEqual(self.search_ids("qty", "=null"), [1])
        self.assertEqual(self.search_ids("qty", "==null"), [1])
        self.assertEqual(self.search_ids("qty", "!=null"), [2, 3])

    def test_null_filters_ignore_spaces_and_case(self):
        self.assertEqual(self.search_ids("qty", " NULL "), [1])
        self.assertEqual(self.search_ids("qty", "==NULL"), [1])
        self.assertEqual(self.search_ids("qty", "!= Null"), [2, 3])

    def test_null_sql_uses_is_null_operators(self):
        is_null_sql = self.statement_sql("qty", "=null")
        is_not_null_sql = self.statement_sql("qty", "!=null")

        self.assertIn("IS NULL", is_null_sql)
        self.assertIn("IS NOT NULL", is_not_null_sql)
        self.assertNotIn("= :", is_null_sql)
        self.assertNotIn("!= :", is_not_null_sql)

    def test_unsupported_null_comparison_is_ignored_before_type_conversion(self):
        rows = (
            self.driver.Pesquisar
            .define_header([["items"], ["id"]])
            .define_filter([["items"], ["created_on"], [">null"]])
            .search(reset=True)
        )

        self.assertEqual([row[0] for row in rows[2:]], [1, 2, 3])

    def test_string_null_filters_use_sql_null_keyword(self):
        self.assertEqual(self.search_ids("name", "null"), [1])
        self.assertEqual(self.search_ids("name", "!=null"), [2, 3])

    def test_string_literal_null_is_reserved_by_filter_syntax(self):
        self.assertEqual(self.search_ids("name", "==null"), [1])
        self.assertNotEqual(self.search_ids("name", "==null"), [2])

    def test_date_and_datetime_null_filters_do_not_raise_conversion_errors(self):
        self.assertEqual(self.search_ids("created_on", "null"), [1])
        self.assertEqual(self.search_ids("created_on", "!=null"), [2, 3])
        self.assertEqual(self.search_ids("updated_at", "null"), [1])
        self.assertEqual(self.search_ids("updated_at", "!=null"), [2, 3])

    def test_null_filters_work_for_boolean_and_numeric_columns(self):
        self.assertEqual(self.search_ids("active", "null"), [1])
        self.assertEqual(self.search_ids("active", "!=null"), [2, 3])
        self.assertEqual(self.search_ids("amount", "null"), [1])
        self.assertEqual(self.search_ids("amount", "!=null"), [2, 3])

    def test_existing_non_null_operators_continue_to_work(self):
        self.assertEqual(self.search_ids("qty", ">10"), [3])
        self.assertEqual(self.search_ids("qty", ">=10"), [2, 3])
        self.assertEqual(self.search_ids("qty", "<20"), [2])
        self.assertEqual(self.search_ids("qty", "<=10"), [2])
        self.assertEqual(self.search_ids("qty", "!=10"), [3])
        self.assertEqual(self.search_ids("qty", "==10"), [2])

    def test_multiple_filters_still_use_and(self):
        rows = (
            self.driver.Pesquisar
            .define_header([["items"], ["id"]])
            .define_filter([
                ["items", "items"],
                ["qty", "name"],
                [">=10", "!=alpha"],
            ])
            .search(reset=True)
        )

        self.assertEqual([row[0] for row in rows[2:]], [2])

    def test_multiple_filter_rows_still_use_or(self):
        rows = (
            self.driver.Pesquisar
            .define_header([["items"], ["id"]])
            .define_filter([
                ["items"],
                ["qty"],
                ["null"],
                ["20"],
            ])
            .search(reset=True)
        )

        self.assertEqual([row[0] for row in rows[2:]], [1, 3])


if __name__ == "__main__":
    unittest.main()