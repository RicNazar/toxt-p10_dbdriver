from __future__ import annotations

from sqlalchemy import text, MetaData, Connection

from .subclasses.DbDriverSearch import DbDriverSearch
from .subclasses.DbDriverUpdate import DbDriverUpdate
from .subclasses.DbDriverUtils import DbDriverUtils, ColumnDefinition
from typing import Any

class DbDriver:
	def __init__(self, metadata: MetaData, connection: Connection):
		self._metadata = metadata
		self._connection = connection
		self.Pesquisar = DbDriverSearch(metadata, connection)
		self.Atualizar = DbDriverUpdate(metadata, connection)

	def execute(self, query: str, dialect: str = "") -> list[list[Any]]:
		# Mantido o parâmetro dialect para compatibilidade com o descritivo.
		del dialect
		with self._connection.begin():
			result = self._connection.execute(text(query))
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount or 0)

	def execute_stmt(self, stmt) -> list[list[Any]]:
		with self._connection.begin():
			result = self._connection.execute(stmt)
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount or 0)

	def get_schema(self) -> dict[str, dict[str, ColumnDefinition]]:
		return DbDriverUtils.get_columns_definitions(self._metadata)