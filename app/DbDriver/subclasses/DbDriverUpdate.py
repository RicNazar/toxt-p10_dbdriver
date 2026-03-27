from __future__ import annotations

from typing import Any, List

from sqlalchemy import delete, insert, select, update

from .DbDriverCore import DbDriverCore
from .DbDriverUtils import DbDriverUtils


class DbDriverUpdate(DbDriverCore):
    def define_data(self, data: List[List[Any]]):
        positions, valid_data = DbDriverUtils.get_valid_columns(self._columns_definitions, data)

        # A coluna MD pode não estar em colunas_definitions, então tratamos manualmente.
        if "MD" in data[1]:
            md_idx_original = data[1].index("MD")
            if md_idx_original not in positions:
                positions.append(md_idx_original)
                positions = sorted(positions)
                valid_data = [[row[i] if i < len(row) else None for i in positions] for row in data]

        self.data_positions = positions
        self.data = valid_data
        return self

    def update(self, reset: bool = True, complete: bool = False, default: Any = None):
        if not hasattr(self, "data"):
            raise ValueError("Data não definida. Use define_data antes de atualizar.")

        data = self.data
        if len(data) < 3:
            raise ValueError("Data inválida. É necessário ao menos uma linha de dados.")
        if "MD" not in data[1]:
            raise ValueError('A última coluna deve se chamar "MD".')

        md_idx = data[1].index("MD")
        table_name = data[0][0]
        table_obj = self._columns_definitions[table_name][data[1][0]]["table_obj"]
        pk_col = self._primary_keys.get(table_name)
        pk_idx = data[1].index(pk_col) if pk_col in data[1] else None
        extra_filter = DbDriverUtils._build_filters(self._columns_definitions, getattr(self, "filter", []))

        with self._engine.begin() as conn:
            for row in data[2:]:
                if md_idx >= len(row):
                    continue

                marker = row[md_idx]
                if marker not in ("U", "A", "D"):
                    raise ValueError(f"Valor inválido na coluna MD: {marker}")

                values = {}
                for idx, col_name in enumerate(data[1]):
                    if col_name == "MD":
                        continue
                    if idx < len(row):
                        values[col_name] = row[idx]

                pk_value = None
                if pk_idx is not None and pk_idx < len(row):
                    pk_value = row[pk_idx]

                if marker == "D":
                    if not pk_col or pk_idx is None or pk_value is None:
                        raise ValueError("Remoção exige chave primária presente no data.")

                    stmt = delete(table_obj).where(table_obj.c[pk_col] == pk_value)
                    if extra_filter is not None:
                        stmt = stmt.where(extra_filter)
                    conn.execute(stmt)
                    continue

                # U/A: tenta update se houver PK, caso contrário faz insert.
                if pk_col and pk_idx is not None and pk_value is not None:
                    update_values = {k: v for k, v in values.items() if k != pk_col}
                    if update_values:
                        stmt = update(table_obj).where(table_obj.c[pk_col] == pk_value).values(**update_values)
                        if extra_filter is not None:
                            stmt = stmt.where(extra_filter)
                        updated = conn.execute(stmt).rowcount or 0
                        if updated > 0:
                            continue

                    # Se o registro já existe pela PK, não deve cair em insert por causa de filtro extra.
                    exists_stmt = select(table_obj.c[pk_col]).where(table_obj.c[pk_col] == pk_value).limit(1)
                    exists = conn.execute(exists_stmt).first() is not None
                    if exists:
                        continue

                insert_values = {k: v for k, v in values.items() if k != "MD"}

                missing_required = []
                for col_name, col_info in self._columns_definitions[table_name].items():
                    if col_name in insert_values:
                        continue
                    if col_info["primary"]:
                        continue
                    has_default = col_info["default"] is not None
                    if not col_info["nullable"] and not has_default:
                        missing_required.append(col_name)

                if missing_required:
                    raise ValueError(
                        "Insert inválido: faltam colunas obrigatórias sem default: "
                        + ", ".join(missing_required)
                    )

                conn.execute(insert(table_obj).values(**insert_values))

        if complete:
            target_tables, target_headers, source_index_map = DbDriverUtils.expand_structure(
                self._columns_definitions,
                data,
                include_md=True,
            )
            output = DbDriverUtils.project_matrix(
                data,
                target_tables,
                target_headers,
                source_index_map,
                default=default,
            )
        else:
            output = data

        if reset:
            self.reset()

        return output
