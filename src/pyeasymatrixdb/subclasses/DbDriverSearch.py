from __future__ import annotations

from typing import Any, List

from .DbDriverCore import DbDriverCore
from .DbDriverUtils import DbDriverUtils


class DbDriverSearch(DbDriverCore):
    def reset(self):
        super().reset()
        for attr in ("table", "header", "filter", "_original_header"):
            if hasattr(self, attr):
                delattr(self, attr)

    def define_header(self, header: List[List[Any]]):
        positions, valid_header = DbDriverUtils.get_valid_columns(self._columns_definitions, header)
        self.header_positions = positions
        self.header = valid_header
        self._original_header = header
        self.table = valid_header[0][0] if valid_header and valid_header[0] else None
        return self

    def search(self, reset: bool = True, complete: bool = False, default: Any = None,approximate: bool = False) -> List[List[Any]]:
        # reset: limpa o estado após a busca
        # complete: retorna todas as colunas do header original (incluindo inválidas), preenchendo ausentes com `default`
        # default: valor para colunas ausentes no resultado da query quando `complete=True`
        # approximate: se True, adiciona * no início e fim das colunas não vazias do filtro para busca aproximada (LIKE '%valor%')

        if not hasattr(self, "header"):
            raise ValueError("Header não definido. Use define_header antes de pesquisar.")

        relationships = getattr(self, "relationships", [])
        filters = getattr(self, "filter", [])

        # Se `approximate` for True, envolve valores string simples com * para busca aproximada
        if approximate and filters and len(filters) >= 3:
            modified = [filters[0], filters[1]]
            for row in filters[2:]:
                new_row = []
                for val in row:
                    # verifica se existe a tabela e coluna
                    valid_table = len(filters) > 0 and len(filters[0]) > 0 and filters[0][0] in self._columns_definitions
                    valid_column = len(filters) > 1 and len(filters[1]) > 0 and filters[1][0] in self._columns_definitions.get(filters[0][0], {})

                    # coleta o tipo de self._columns_definitions e verifica se é string não vazia sem operadores de comparação
                    if valid_table and valid_column:
                        column_type = str(self._columns_definitions[filters[0][0]][filters[0][1]]["column_obj"].type).upper()
                        is_text =  any(t in column_type for t in ("VARCHAR", "CHAR", "TEXT", "CLOB", "STRING", "NCHAR", "NVARCHAR", "UNICODE", "ENUM"))
                        if is_text and val.strip() and not val.startswith(("!=", ">=", "<=", ">", "<", "*")) and not val.endswith("*"):
                            new_row.append(f"*{val.strip()}*")
                        else:
                            new_row.append(val)
                    else:
                        new_row.append(val)
                modified.append(new_row)
            filters = modified


        # monta e executa o SELECT com apenas as colunas válidas
        stmt = DbDriverUtils.buid_select(
            columns_definitions= self._columns_definitions,
            headers=self.header,
            relationships=relationships,
            filters=filters,
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            records = [list(row) for row in result]

        if complete:
            # usa o header original (pode conter colunas inválidas/ausentes no schema)
            full_header = getattr(self, "_original_header", self.header)
            # mapeia posição original → índice no resultado da query
            pos_to_result_idx = {pos: idx for idx, pos in enumerate(self.header_positions)}
            n_cols = len(full_header[0])
            output_rows = []
            for record in records:
                row = []
                for i in range(n_cols):
                    result_idx = pos_to_result_idx.get(i, -1)
                    # coluna inexistente no schema recebe `default`
                    row.append(record[result_idx] if result_idx != -1 and result_idx < len(record) else default)
                output_rows.append(row)
            output = [full_header[0], full_header[1], *output_rows]
        else:
            # retorna apenas as colunas válidas buscadas
            output = [self.header[0], self.header[1], *records]

        if reset:
            self.reset()

        return output
