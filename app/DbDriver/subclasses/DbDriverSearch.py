from __future__ import annotations

from typing import Any, List

from .DbDriverCore import DbDriverCore
from .DbDriverUtils import DbDriverUtils


class DbDriverSearch(DbDriverCore):
    def reset(self):
        super().reset()
        for attr in ("table", "header", "filter"):
            if hasattr(self, attr):
                delattr(self, attr)

    def define_header(self, header: List[List[Any]]):
        positions, valid_header = DbDriverUtils.get_valid_columns(self._columns_definitions, header)
        self.header_positions = positions
        self.header = valid_header
        self.table = valid_header[0][0] if valid_header and valid_header[0] else None
        return self

    def search(self, reset: bool = True, complete: bool = False, default: Any = None):
        if not hasattr(self, "header"):
            raise ValueError("Header não definido. Use define_header antes de pesquisar.")

        relationships = getattr(self, "relationships", [])
        filters = getattr(self, "filter", [])

        stmt = DbDriverUtils.buid_select(
            self._columns_definitions,
            self.header,
            relationships=relationships,
            filters=filters,
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            records = [list(row) for row in result]

        if complete:
            target_tables, target_headers, source_index_map = DbDriverUtils.expand_structure(
                self._columns_definitions,
                self.header,
                include_md=False,
            )
            base_matrix = [self.header[0], self.header[1], *records]
            output = DbDriverUtils.project_matrix(
                base_matrix,
                target_tables,
                target_headers,
                source_index_map,
                default=default,
            )
        else:
            output = [self.header[0], self.header[1], *records]

        if reset:
            self.reset()

        return output
