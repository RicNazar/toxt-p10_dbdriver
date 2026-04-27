from __future__ import annotations

import re
from typing import Any, List

from sqlalchemy import and_, bindparam, delete, insert, select, update

from .DbDriverCore import DbDriverCore
from .DbDriverUtils import DbDriverUtils


class DbDriverUpdate(DbDriverCore):
    # Acima deste volume, vale a pena trocar o loop linha a linha por batch.
    _BATCH_UPDATE_THRESHOLD = 20
    _BATCH_CHUNK_SIZE = 900

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

    def _fill_first_integer_column(
        self,
        data: List[List[Any]],
        table_name: str,
        first_header: str,
        md_idx: int,
    ) -> None:
        first_column_info = self._columns_definitions[table_name][first_header]
        first_column_type = str(first_column_info["type"]).upper()
        if "INT" not in first_column_type:
            return

        table_obj = first_column_info["table_obj"]
        current_max = conn_max = None

        with self._engine.begin() as conn:
            conn_max = conn.execute(
                select(table_obj.c[first_header]).order_by(table_obj.c[first_header].desc()).limit(1)
            ).scalar()

        explicit_values = [
            row[0]
            for row in data[2:]
            if row
            and len(row) > 0
            and row[0] is not None
            and (md_idx >= len(row) or str(row[md_idx]) != "D")
        ]

        numeric_values = [int(value) for value in explicit_values if isinstance(value, int)]
        if conn_max is not None:
            numeric_values.append(int(conn_max))

        current_max = max(numeric_values, default=0)

        for row in data[2:]:
            if not row or len(row) <= 0:
                continue
            if md_idx < len(row) and str(row[md_idx]) == "D":
                continue
            if row[0] is not None:
                continue

            current_max += 1
            row[0] = current_max

    def _should_use_batch_upsert(
        self,
        data: List[List[Any]],
        headers: List[str],
        md_idx: int,
        pk_col: Any,
    ) -> bool:
        # O batch só entra no caso simples: A/U com PK presente.
        if pk_col is None or pk_col not in headers or len(data[2:]) <= self._BATCH_UPDATE_THRESHOLD:
            return False

        for row in data[2:]:
            if md_idx >= len(row):
                continue

            raw_marker = str(row[md_idx]) if row[md_idx] is not None else ""
            m = re.fullmatch(r"([AUD])(\d+)?", raw_marker)
            if not m or m.group(1) == "D" or m.group(2) is not None:
                return False

        return True

    def _batch_upsert_by_pk(
        self,
        data: List[List[Any]],
        table_name: str,
        table_obj,
        headers: List[str],
        md_idx: int,
        pk_col: str,
        extra_filter,
    ) -> List[Any]:
        rows_with_pk: List[tuple[int, dict[str, Any]]] = []
        rows_without_pk: List[tuple[int, dict[str, Any]]] = []

        for row_offset, row in enumerate(data[2:]):
            if md_idx >= len(row):
                continue

            raw_marker = str(row[md_idx]) if row[md_idx] is not None else ""
            m = re.fullmatch(r"([AUD])(\d+)?", raw_marker)
            if not m or m.group(1) == "D" or m.group(2) is not None:
                raise ValueError(f"Valor inválido na coluna MD: {raw_marker}")

            values = {}
            for idx, col_name in enumerate(headers):
                if idx < len(row):
                    col_type = str(self._columns_definitions[table_name][col_name]["type"])
                    val, _ = DbDriverUtils._valid_info(col_type, row[idx])
                    values[col_name] = val

            pk_value = values.get(pk_col)
            if pk_value is None:
                values_without_pk = dict(values)
                values_without_pk.pop(pk_col, None)
                rows_without_pk.append((row_offset, values_without_pk))
                continue

            rows_with_pk.append((row_offset, values))

        if not rows_with_pk and not rows_without_pk:
            return []

        with self._engine.begin() as conn:
            # Descobre quais PKs já existem antes de separar update de insert.
            existing_pks = set()
            batch_pks = [values[pk_col] for _, values in rows_with_pk]

            for start in range(0, len(batch_pks), self._BATCH_CHUNK_SIZE):
                chunk = batch_pks[start:start + self._BATCH_CHUNK_SIZE]
                if not chunk:
                    continue
                existing_pks.update(
                    row[0]
                    for row in conn.execute(
                        select(table_obj.c[pk_col]).where(table_obj.c[pk_col].in_(chunk))
                    )
                )

            known_pks = set(existing_pks)
            update_groups: dict[tuple[str, ...], List[dict[str, Any]]] = {}
            insert_groups: dict[tuple[str, ...], List[tuple[int, dict[str, Any]]]] = {}
            generated_insert_rows: List[tuple[int, dict[str, Any]]] = []
            result_map: dict[int, Any] = {}

            for row_offset, values in rows_with_pk:
                pk_value = values[pk_col]
                set_keys = tuple(k for k in values if k != pk_col)

                if pk_value in known_pks:
                    if set_keys:
                        # Agrupa updates com o mesmo conjunto de colunas para usar executemany.
                        params = {"pk_match": pk_value, **{k: values[k] for k in set_keys}}
                        update_groups.setdefault(set_keys, []).append(params)
                    result_map[row_offset] = pk_value
                else:
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

                    # Agrupa inserts com a mesma estrutura de colunas.
                    insert_groups.setdefault(tuple(values.keys()), []).append((row_offset, values))
                    known_pks.add(pk_value)
                    result_map[row_offset] = pk_value

            for row_offset, values in rows_without_pk:
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
                generated_insert_rows.append((row_offset, values))

            for set_keys, params_list in update_groups.items():
                stmt = update(table_obj).where(table_obj.c[pk_col] == bindparam("pk_match"))
                if extra_filter is not None:
                    stmt = stmt.where(extra_filter)
                stmt = stmt.values({col: bindparam(col) for col in set_keys})
                conn.execute(stmt, params_list)

            for _, rows in insert_groups.items():
                conn.execute(insert(table_obj), [values for _, values in rows])

            for row_offset, values in generated_insert_rows:
                result = conn.execute(insert(table_obj).values(**values))
                new_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
                result_map[row_offset] = new_id

        return [result_map[row_offset] for row_offset in sorted(result_map)]

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

        table_name = str(data[0][0])
        first_header = str(data[1][0])
        self._fill_first_integer_column(data, table_name, first_header, md_idx)
        table_columns = self._columns_definitions[table_name]
        table_obj = table_columns[first_header]["table_obj"]
        pk_col_raw = self._primary_keys.get(table_name)
        pk_col = str(pk_col_raw) if pk_col_raw is not None else None
        pk_idx = data[1].index(pk_col) if pk_col and pk_col in data[1] else None
        headers: List[str] = [str(header) for header in data[1][:md_idx]]  # colunas sem MD

        # Filtro extra definido via define_filter
        extra_filter = DbDriverUtils._build_filters(
            self._columns_definitions, getattr(self, "filter", [])
        )

        # Mantém o fluxo antigo como fallback e usa batch só no caso mais previsível.
        if self._should_use_batch_upsert(data, headers, md_idx, pk_col):
            if pk_col is None:
                raise ValueError("Chave primária não definida para batch update.")
            result_ids = self._batch_upsert_by_pk(
                data, table_name, table_obj, headers, md_idx, pk_col, extra_filter
            )
            if reset:
                self.reset()
            return result_ids

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
