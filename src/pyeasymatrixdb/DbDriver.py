from __future__ import annotations

from sqlalchemy import text, Engine, MetaData

from .subclasses.DbDriverSearch import DbDriverSearch
from .subclasses.DbDriverUpdate import DbDriverUpdate
from .subclasses.DbDriverUtils import DbDriverUtils, ColumnDefinition
from typing import Dict, List, Tuple, Any




class DbDriver:
	def __init__(self, metadata: MetaData, engine: Engine):
		self._metadata = metadata
		self._engine = engine
		self.Pesquisar = DbDriverSearch(metadata, engine)
		self.Atualizar = DbDriverUpdate(metadata, engine)

	def execute(self, query: str, dialect: str = "") -> list[list[any]]:
		# Mantido o parâmetro dialect para compatibilidade com o descritivo.
		del dialect
		with self._engine.begin() as conn:
			result = conn.execute(text(query))
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount or 0)

	def execute_stmt(self, stmt) -> list[list[any]]:
		with self._engine.begin() as conn:
			result = conn.execute(stmt)
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount or 0)

	def get_schema(self) -> Dict[str, Dict[str, ColumnDefinition]]:
		return DbDriverUtils.get_columns_definitions(self._metadata)