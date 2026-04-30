from __future__ import annotations

from typing import Any, List

from .DbDriverUtils import DbDriverUtils

from sqlalchemy import Engine, MetaData


class DbDriverCore:
    def __init__(self, metadata:MetaData, engine:Engine):
        self._metadata = metadata
        self._engine = engine
        self._columns_definitions = DbDriverUtils.get_columns_definitions(metadata)
        self._primary_keys = DbDriverUtils.get_primary_keys(metadata)

    def reset(self):
        for attr in list(vars(self).keys()):
            if attr.startswith("_"):
                continue
            delattr(self, attr)

    def define_filter(self, filter: List[List[Any]],debug: bool = False):
        positions, valid_filter = DbDriverUtils.get_valid_columns(columns_definitions= self._columns_definitions,matrix = filter,debug=debug)
        self.filter_positions = positions
        self.filter = valid_filter
        return self

    def define_relationships(self, relationships: List[List[Any]]):
        if relationships is None:
            self.relationships = []
            return self

        valid_relationships = []
        for rel in relationships:
            if len(rel) < 4:
                raise ValueError("Relacionamento inválido. Esperado ao menos 4 colunas.")

            table_a, table_b, col_a, col_b = rel[0], rel[1], rel[2], rel[3]

            if not DbDriverUtils.is_valid_table(self._columns_definitions, table_a):
                raise ValueError(f"Tabela inválida em relacionamento: {table_a}")
            if not DbDriverUtils.is_valid_table(self._columns_definitions, table_b):
                raise ValueError(f"Tabela inválida em relacionamento: {table_b}")
            if col_a not in self._columns_definitions[table_a]:
                raise ValueError(f"Coluna inválida em relacionamento: {table_a}.{col_a}")
            if col_b not in self._columns_definitions[table_b]:
                raise ValueError(f"Coluna inválida em relacionamento: {table_b}.{col_b}")

            valid_relationships.append(rel)

        self.relationships = valid_relationships
        return self
