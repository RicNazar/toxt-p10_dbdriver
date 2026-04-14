from __future__ import annotations

import re
from typing import Any, List

from sqlalchemy import and_, delete, insert, select, update

from .DbDriverCore import DbDriverCore
from .DbDriverUtils import DbDriverUtils


class DbDriverUpdate(DbDriverCore):
    def define_data(self, data: List[List[Any]]):
        # MD deve ser a última coluna e não pode ser a única
        if not data or len(data) < 2 or len(data[1]) < 2 or data[1][-1] != "MD":
            raise ValueError('A coluna "MD" deve ser a última coluna e não pode ser a única.')

        md_idx = len(data[1]) - 1

        # Filtra posições válidas (ignora colunas inexistentes na tabela)
        positions = [
            idx for idx in range(md_idx)
            if data[0][idx] in self._columns_definitions
            and data[1][idx] in self._columns_definitions[data[0][idx]]
        ]

        if not positions:
            raise ValueError("Nenhuma coluna válida encontrada na data.")

        # MD sempre ao final
        positions.append(md_idx)

        self.data_positions = positions
        self.data = [[row[i] if i < len(row) else None for i in positions] for row in data]
        return self

    def update(self, reset: bool = True) -> List[Any]:
        if not hasattr(self, "data"):
            raise ValueError("Data não definida. Use define_data antes de atualizar.")

        data = self.data
        if len(data) < 3:
            raise ValueError("Data inválida. É necessário ao menos uma linha de dados.")

        # MD é sempre a última coluna após define_data
        md_idx = len(data[1]) - 1
        if data[1][md_idx] != "MD":
            raise ValueError('A coluna "MD" deve ser a última coluna.')

        table_name = data[0][0]
        table_obj = self._columns_definitions[table_name][data[1][0]]["table_obj"]
        pk_col = self._primary_keys.get(table_name)
        pk_idx = data[1].index(pk_col) if pk_col and pk_col in data[1] else None
        headers = data[1][:md_idx]  # colunas sem MD

        # Filtro extra definido via define_filter
        extra_filter = DbDriverUtils._build_filters(
            self._columns_definitions, getattr(self, "filter", [])
        )

        result_ids: List[Any] = []

        with self._engine.begin() as conn:
            for row in data[2:]:
                if md_idx >= len(row):
                    continue

                raw_marker = str(row[md_idx]) if row[md_idx] is not None else ""
                # Extrai base (A/U/D) e número opcional (ex: "U2" → base="U", n=2)
                m = re.fullmatch(r"([AUD])(\d+)?", raw_marker)
                if not m:
                    raise ValueError(f"Valor inválido na coluna MD: {raw_marker}")

                base = m.group(1)
                n = int(m.group(2)) if m.group(2) else None

                # Converte os valores usando _valid_info para garantir tipos corretos
                values = {}
                for idx, col_name in enumerate(headers):
                    if idx < len(row):
                        col_type = str(self._columns_definitions[table_name][col_name]["type"])
                        val, _ = DbDriverUtils._valid_info(col_type, row[idx])
                        values[col_name] = val

                pk_value = values.get(pk_col) if pk_col else None

                # --- DELETE ---
                if base == "D":
                    if pk_idx is not None and pk_value is not None:
                        # Remoção por PK
                        stmt = delete(table_obj).where(table_obj.c[pk_col] == pk_value)
                    else:
                        # Remoção por todas as colunas passadas com valor
                        conds = [table_obj.c[c] == v for c, v in values.items() if v is not None]
                        if not conds:
                            continue
                        stmt = delete(table_obj).where(and_(*conds))
                    if extra_filter is not None:
                        stmt = stmt.where(extra_filter)
                    conn.execute(stmt)
                    continue

                # --- UPSERT (A/U) ---
                if pk_idx is not None and pk_value is not None:
                    # PK presente: tenta update, senão verifica existência e insere
                    set_vals = {k: v for k, v in values.items() if k != pk_col}
                    if set_vals:
                        upd = update(table_obj).where(table_obj.c[pk_col] == pk_value).values(**set_vals)
                        if extra_filter is not None:
                            upd = upd.where(extra_filter)
                        updated = conn.execute(upd).rowcount or 0
                        if updated > 0:
                            result_ids.append(pk_value)
                            continue

                    # Verifica existência para evitar inserção duplicada
                    exists = conn.execute(
                        select(table_obj.c[pk_col]).where(table_obj.c[pk_col] == pk_value).limit(1)
                    ).first() is not None
                    if exists:
                        result_ids.append(pk_value)
                        continue

                elif n is not None:
                    # n primeiras colunas são WHERE, restante é SET
                    if n <= 0 or n >= len(headers):
                        raise ValueError(f"Número de colunas where inválido em '{raw_marker}'.")
                    where_conds = [table_obj.c[c] == values[c] for c in headers[:n] if c in values]
                    set_vals = {c: values[c] for c in headers[n:] if c in values and values[c] is not None}
                    if not where_conds or not set_vals:
                        continue

                    upd = update(table_obj).where(and_(*where_conds)).values(**set_vals)
                    if extra_filter is not None:
                        upd = upd.where(extra_filter)
                    updated = conn.execute(upd).rowcount or 0
                    if updated > 0:
                        result_ids.append(pk_value)
                        continue

                else:
                    raise ValueError(
                        f"Marcador '{raw_marker}' sem PK nos dados requer número de colunas where (ex: A2, U2)."
                    )

                # INSERT (chegou aqui quando update não encontrou linha)
                missing = [
                    c for c, info in self._columns_definitions[table_name].items()
                    if c not in values
                    and not info["primary"]
                    and not info["nullable"]
                    and info["default"] is None
                ]
                if missing:
                    raise ValueError(
                        "Insert inválido: faltam colunas obrigatórias sem default: "
                        + ", ".join(missing)
                    )

                result = conn.execute(insert(table_obj).values(**values))
                new_id = result.inserted_primary_key[0] if result.inserted_primary_key else pk_value
                result_ids.append(new_id)

        if reset:
            self.reset()

        return result_ids
