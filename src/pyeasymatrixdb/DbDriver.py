from __future__ import annotations

from sqlalchemy import text, Engine, MetaData

from .subclasses.DbDriverSearch import DbDriverSearch
from .subclasses.DbDriverUpdate import DbDriverUpdate
from .subclasses.DbDriverUtils import DbDriverUtils


class DbDriver:
	def __init__(self, metadata: MetaData, engine: Engine):
		self._metadata = metadata
		self._engine = engine
		self.Pesquisar = DbDriverSearch(metadata, engine)
		self.Atualizar = DbDriverUpdate(metadata, engine)

	def execute(self, query: str, dialect: str = ""):
		# Mantido o parâmetro dialect para compatibilidade com o descritivo.
		del dialect
		with self._engine.begin() as conn:
			result = conn.execute(text(query))
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount)

	def execute_stmt(self, stmt):
		with self._engine.begin() as conn:
			result = conn.execute(stmt)
			if result.returns_rows:
				columns = list(result.keys())
				records = [list(row) for row in result]
				return DbDriverUtils.to_matrix_from_records(columns, records)
			return DbDriverUtils.to_meta_matrix(result.rowcount)
