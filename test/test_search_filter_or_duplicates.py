from pathlib import Path
import sys
import unittest

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyeasymatrixdb import DbDriver


class SearchFilterDuplicateColumnTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        metadata = MetaData()
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), nullable=False),
        )
        metadata.create_all(self.engine)
        self.driver = DbDriver(metadata, self.engine)
        self.driver.execute(
            """
            INSERT INTO users (id, name) VALUES
            (1, 'Ana'),
            (2, 'Bruno'),
            (3, 'Carla')
            """
        )

    def test_duplicate_column_without_operator_uses_or(self):
        rows = (
            self.driver.Pesquisar
            .define_header([["users", "users"], ["id", "name"]])
            .define_filter([
                ["users", "users"],
                ["name", "name"],
                ["Ana", "Bruno"],
            ])
            .search()
        )

        self.assertEqual(rows[0], ["users", "users"])
        self.assertEqual(rows[1], ["id", "name"])
        self.assertEqual(rows[2:], [[1, "Ana"], [2, "Bruno"]])

    def test_duplicate_column_with_operator_keeps_and(self):
        rows = (
            self.driver.Pesquisar
            .define_header([["users", "users"], ["id", "name"]])
            .define_filter([
                ["users", "users"],
                ["id", "id"],
                [">1", "<3"],
            ])
            .search()
        )

        self.assertEqual(rows[2:], [[2, "Bruno"]])


if __name__ == "__main__":
    unittest.main()
