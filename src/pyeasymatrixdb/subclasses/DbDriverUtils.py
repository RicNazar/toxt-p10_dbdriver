from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Tuple, TypedDict

from sqlalchemy import MetaData, and_, delete, exists, insert, literal, or_, select, update

class ColumnDefinition(TypedDict):
    type: Any
    primary: bool
    unique: bool
    default: Any
    nullable: bool
    table_obj: Any
    column_obj: Any


class DbDriverUtils:
    def __new__(cls, *args, **kwargs):
        raise TypeError("This class cannot be instantiated")

    @staticmethod
    def get_columns_definitions(metadata: MetaData) -> Dict[str, Dict[str, ColumnDefinition]]:
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]] = {}

        for table_name, table_obj in metadata.tables.items():
            columns_definitions[table_name] = {}
            for col in table_obj.columns:
                default_value = None
                if col.default is not None:
                    default_value = getattr(col.default, "arg", col.default)

                columns_definitions[table_name][col.name] = {
                    "type": col.type,
                    "primary": bool(col.primary_key),
                    "unique": bool(col.unique),
                    "default": default_value,
                    "nullable": bool(col.nullable),
                    "table_obj": table_obj,
                    "column_obj": col,
                }

        return columns_definitions

    @staticmethod
    def get_primary_keys(metadata: MetaData) -> Dict[str, Any]:
        primary_keys: Dict[str, Any] = {}
        for table_name, table_obj in metadata.tables.items():
            pks = [col.name for col in table_obj.primary_key.columns]
            primary_keys[table_name] = pks[0] if pks else None
        return primary_keys

    @staticmethod
    def get_valid_columns(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        matrix: List[List[Any]],
    ) -> Tuple[List[int], List[List[Any]]]:
        #valida se a matriz tem ao menos 2 linhas
        if not matrix or len(matrix) < 2:
            raise ValueError("Matriz inválida. É necessário ter ao menos 2 linhas.")

        #separa a linha de tabelas e a linha de colunas
        table_row = matrix[0]
        header_row = matrix[1]

        #erro caso o número de colunas seja diferente entre a linha de tabelas e a linha de colunas
        if len(table_row) != len(header_row):
            raise ValueError("Matriz inválida. O número de colunas na linha de tabelas e na linha de colunas deve ser o mesmo.")
        
        max_cols = len(header_row)

        valid_positions: List[int] = []
        for idx in range(max_cols):
            table_name = table_row[idx]
            column_name = header_row[idx]

            if (
                table_name in columns_definitions
                and column_name in columns_definitions[table_name]
            ):
                valid_positions.append(idx)

        if not valid_positions:
            raise ValueError("Nenhuma coluna válida foi encontrada na matriz.")

        #monta a matriz com as colunas válidas
        valid_matrix = [[row[i] for i in valid_positions] for row in matrix]
        return valid_positions, valid_matrix

    @staticmethod
    def is_valid_table(columns_definitions: Dict[str, Dict[str, ColumnDefinition]], table: str) -> bool:
        return table in columns_definitions

    @staticmethod
    def buid_select(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        headers: List[List[Any]],
        relationships: List[List[Any]] = [],
        filters: List[List[Any]] = [],
    ):
        # Valida se headers tem ao menos 2 linhas (tabelas e colunas)
        if not headers or len(headers) < 2:
            raise ValueError("Header inválido para select.")

        # Coleta os objetos de coluna do SQLAlchemy para cada coluna solicitada
        columns = []
        for idx, column_name in enumerate(headers[1]):
            table_name = headers[0][idx]
            table_obj = columns_definitions[table_name][column_name]["table_obj"]
            columns.append(table_obj.c[column_name])

        # Define a tabela base para a consulta
        base_table = columns_definitions[headers[0][0]][headers[1][0]]["table_obj"]
        from_clause = base_table

        # Aplica joins baseado nos relacionamentos fornecidos
        first_join_table = True
        for rel in reversed(relationships or []):
            if len(rel) < 4:
                continue

            table_a, table_b, col_a, col_b = rel[0], rel[1], rel[2], rel[3]
            inner = True if len(rel) < 5 else bool(rel[4])
            
            table_a_obj = columns_definitions[table_a][col_a]["table_obj"]
            table_b_obj = columns_definitions[table_b][col_b]["table_obj"]
            condition = table_a_obj.c[col_a] == table_b_obj.c[col_b]

            # Altera o select from para a primeira tabela do join, garantindo que a ordem dos joins seja respeitada
            if first_join_table:
                from_clause = table_a_obj
                first_join_table = False

            # Aplica join interno ou externo
            if inner:
                from_clause = from_clause.join(table_b_obj, condition)
            else:
                from_clause = from_clause.outerjoin(table_b_obj, condition)

        # Constrói o statement SELECT
        stmt = select(*columns).select_from(from_clause)
        
        # Adiciona filtros se fornecidos
        filter_condition = DbDriverUtils._build_filters(columns_definitions, filters)
        if filter_condition is not None:
            stmt = stmt.where(filter_condition)

        return stmt

    @staticmethod
    def buid_update(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        data: List[List[Any]],
        relationships: List[List[Any]] = [],
        filters: List[List[Any]] = [],
    ):
        #valida se a matriz tem 3 linhas ou mais
        if not data or len(data) < 3:
            raise ValueError("Dados inválidos para update.")

        table_name = data[0][0]
        #valida se toda a primeira linha tem o mesmo nome de tabela
        if any(t != table_name for t in data[0]):
            raise ValueError("Dados inválidos para update. A primeira linha deve conter o mesmo nome de tabela.")

        #coleta a tabela e o índice da coluna MD (se existir)
        table_obj = columns_definitions[table_name][data[1][0]]["table_obj"]
        md_idx = len(data[1])-1
        if  data[1][md_idx] != "MD":
            raise ValueError("A coluna MD é obrigatória.")

        pk_col = None
        for col_name, info in columns_definitions[table_name].items():
            if info["primary"]:
                pk_col = col_name
                break
        
        #testa se a pk está presente no header
        pk_idx = None
        if pk_col:
            if pk_col in data[1]:
                pk_idx = data[1].index(pk_col)

        #valida as possibilidades filter sem pk (com ou sem relationship), ou apenas pk sem filter/relationship
        if pk_idx is None and not filters and not relationships:
            raise ValueError("Dados inválidos para update. É necessário ter uma chave primária ou um filtro/relacionamento para identificar as linhas a serem atualizadas.")
        if pk_idx is None:
            if not filter:
                raise ValueError("Dados inválidos para update. Filtro obrigatório quando não há chave primária.")
        else:
            if filters or relationships:
                raise ValueError("Dados inválidos para update. Não é permitido usar filtros ou relacionamentos quando a chave primária está presente.")

        #monta as regras
        stmts = []
        for row in data[2:]:
            #verifica se precisa fazer algo pela coluna MD
            marker = row[md_idx]
            if marker not in ("U", "A"):
                continue
            
            #inicia a montagem do update, ignorando colunas MD e PK
            values = {}
            for idx, col_name in enumerate(data[1]):
                if col_name in ("MD", pk_col):
                    continue
                if idx < len(row) and row[idx] is not None:
                    values[col_name] = row[idx]

            if values:
                stmt = update(table_obj)
                if pk_idx is not None:
                    stmt = stmt.where(table_obj.c[pk_col] == row[pk_idx]).values(**values)
                else:
                    if len(values) != 1:
                        raise ValueError("Dados inválidos para update. Quando não há chave primária, deve haver exatamente uma linha com atualizações.")
                    extra_filter = DbDriverUtils._build_filters(columns_definitions, filters)
                    if extra_filter is None:
                        raise ValueError("Dados inválidos para update. Filtro inválido ou sem correspondência.")
                    relationships_filter = None
                    if relationships:
                        for rel in reversed(relationships):
                            if len(rel) < 4:
                                continue

                            table_a, table_b, col_a, col_b = rel[0], rel[1], rel[2], rel[3]
                            table_a_obj = columns_definitions[table_a][col_a]["table_obj"]
                            table_b_obj = columns_definitions[table_b][col_b]["table_obj"]
                            condition = table_a_obj.c[col_a] == table_b_obj.c[col_b]

                            if relationships_filter is None:
                                relationships_filter = condition
                            else:
                                relationships_filter = and_(relationships_filter, condition)
                    if relationships_filter is not None:
                        stmt = stmt.where(relationships_filter)
                    stmt = stmt.where(extra_filter).values(**values)
                stmts.append(stmt)

        return stmts

    @staticmethod
    def buid_insert(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        data: List[List[Any]],
        filters: List[List[Any]] = [],
    ):
        if not data or len(data) < 3:
            raise ValueError("Dados inválidos para insert.")

        table_name = data[0][0]
        table_obj = columns_definitions[table_name][data[1][0]]["table_obj"]
        md_idx = len(data[1]) - 1

        rows = []
        for row in data[2:]:
            # Ignora linhas marcadas para remoção
            if md_idx != -1 and md_idx < len(row) and row[md_idx] == "D":
                continue

            values = {}
            for idx, col_name in enumerate(data[1]):
                if col_name == "MD":
                    continue
                if idx < len(row):
                    # Converte o valor para o tipo correto da coluna
                    col_type = str(columns_definitions[table_name][col_name]["type"])
                    val, _ = DbDriverUtils._valid_info(col_type, row[idx])
                    values[col_name] = val
            rows.append(values)

        if not rows:
            return None

        # Se filtro fornecido, insere apenas onde nenhuma linha correspondente existe
        filter_cond = DbDriverUtils._build_filters(columns_definitions, filters)
        if filter_cond is not None:
            first_col = next(iter(table_obj.c))
            stmts = []
            for rv in rows:
                col_names = list(rv.keys())
                not_exists = ~exists(select(first_col).where(filter_cond))
                sel = select(*[literal(v).label(c) for c, v in rv.items()]).where(not_exists)
                stmts.append(insert(table_obj).from_select(col_names, sel))
            return stmts

        return insert(table_obj).values(rows)

    @staticmethod
    def buid_delete(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        keys: List[List[Any]] = [],
        relationships: List[List[Any]] = [],
        filters: List[List[Any]] = [],
    ):
        if not keys or len(keys) < 3:
            raise ValueError("Dados inválidos para delete.")

        table_name = keys[0][0]
        table_obj = columns_definitions[table_name][keys[1][0]]["table_obj"]
        stmts = []

        for row in keys[2:]:
            row_filters = [keys[0], keys[1], row]
            cond = DbDriverUtils._build_filters(columns_definitions, row_filters)
            if cond is None:
                continue

            stmt = delete(table_obj).where(cond)
            extra_filter = DbDriverUtils._build_filters(columns_definitions, filters)
            if extra_filter is not None:
                stmt = stmt.where(extra_filter)
            stmts.append(stmt)

        return stmts

    @staticmethod
    def _build_filters(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        filters: List[List[Any]],
    ):
        if not filters or len(filters) < 3:
            return None

        row_conditions = []
        for row in filters[2:]:
            col_conditions = []
            for idx, raw_value in enumerate(row):
                if idx >= len(filters[0]) or idx >= len(filters[1]):
                    continue
                if not raw_value:
                    continue

                table_name = filters[0][idx]
                col_name = filters[1][idx]
                column = columns_definitions[table_name][col_name]["column_obj"]
                value, _value_type = DbDriverUtils._valid_info(str(column.type), raw_value)

                if isinstance(raw_value, tuple) and len(raw_value) == 2:
                    op, val = raw_value
                    val, val_type = DbDriverUtils._valid_info(str(column.type), val)
                    if op == "!=":
                        # TEXT com wildcard vira NOT LIKE
                        if val_type == "TEXT" and isinstance(val, str):
                            _w_starts = val.startswith("*")
                            _w_ends = val.endswith("*")
                            if _w_starts or _w_ends:
                                _inner = val.lstrip("*").rstrip("*")
                                _like = ("%" if _w_starts else "") + _inner + ("%" if _w_ends else "")
                                col_conditions.append(column.notlike(_like))
                            else:
                                col_conditions.append(column != val)
                        else:
                            col_conditions.append(column != val)
                    elif op == ">":
                        col_conditions.append(column > val)
                    elif op == ">=":
                        col_conditions.append(column >= val)
                    elif op == "<":
                        col_conditions.append(column < val)
                    elif op == "<=":
                        col_conditions.append(column <= val)
                    elif op == "like":
                        col_conditions.append(column.like(val))
                    else:
                        # igualdade: TEXT com wildcard vira LIKE
                        if val_type == "TEXT" and isinstance(val, str):
                            _w_starts = val.startswith("*")
                            _w_ends = val.endswith("*")
                            if _w_starts or _w_ends:
                                _inner = val.lstrip("*").rstrip("*")
                                _like = ("%" if _w_starts else "") + _inner + ("%" if _w_ends else "")
                                col_conditions.append(column.like(_like))
                            else:
                                col_conditions.append(column == val)
                        else:
                            col_conditions.append(column == val)
                else:
                    # operadores inline em string: ">=1", "!=OPEN", "<5", etc.
                    _str_op = None
                    _str_val = raw_value
                    if isinstance(raw_value, str):
                        for _op in ("!=", ">=", "<=", ">", "<", "=="):
                            if raw_value.startswith(_op):
                                _str_op = _op
                                _str_val = raw_value[len(_op):]
                                break
                    if _str_op is not None:
                        parsed_val, parsed_type = DbDriverUtils._valid_info(str(column.type), _str_val)
                        if _str_op == "!=":
                            # TEXT com wildcard vira NOT LIKE
                            if parsed_type == "TEXT" and isinstance(parsed_val, str):
                                _w_starts = parsed_val.startswith("*")
                                _w_ends = parsed_val.endswith("*")
                                if _w_starts or _w_ends:
                                    _inner = parsed_val.lstrip("*").rstrip("*")
                                    _like = ("%" if _w_starts else "") + _inner + ("%" if _w_ends else "")
                                    col_conditions.append(column.notlike(_like))
                                else:
                                    col_conditions.append(column != parsed_val)
                            else:
                                col_conditions.append(column != parsed_val)
                        elif _str_op == ">":
                            col_conditions.append(column > parsed_val)
                        elif _str_op == ">=":
                            col_conditions.append(column >= parsed_val)
                        elif _str_op == "<":
                            col_conditions.append(column < parsed_val)
                        elif _str_op == "<=":
                            col_conditions.append(column <= parsed_val)
                        elif _str_op == "==":
                            if _str_val == "":
                                if parsed_type == "TEXT":
                                    col_conditions.append(or_(column.is_(None), column == ""))
                                else:
                                    col_conditions.append(column.is_(None))
                            else:
                                col_conditions.append(column == parsed_val)
                    else:
                        # wildcard em campos de texto: * apenas no início e/ou fim vira LIKE
                        _starts = isinstance(raw_value, str) and raw_value.startswith("*")
                        _ends = isinstance(raw_value, str) and raw_value.endswith("*")
                        if isinstance(raw_value, str) and (_starts or _ends):
                            if _value_type == "TEXT":
                                inner = raw_value.lstrip("*").rstrip("*")
                                like_val = ("%" if _starts else "") + inner + ("%" if _ends else "")
                                col_conditions.append(column.like(like_val))
                            else:
                                col_conditions.append(column == value)
                        else:
                            col_conditions.append(column == value)

            if col_conditions:
                row_conditions.append(and_(*col_conditions))

        if not row_conditions:
            return None
        return or_(*row_conditions)

    @staticmethod
    def _valid_info(type: str, info: Any) -> tuple[Any, str]:
        if info is None:
            return None, "OTHER"

        type_upper = type.upper().split("(")[0].strip()  # remove detalhes de tipo, ex: VARCHAR(255) → VARCHAR
        try:
            # INTERVAL antes de INT para evitar falso match
            if "INTERVAL" in type_upper:
                return info, "OTHER"
            # NUMBER
            if "INT" in type_upper:
                return int(info), "NUMBER"
            if any(t in type_upper for t in ("FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL")):
                return float(info), "NUMBER"
            # BOOLEAN
            if "BOOL" in type_upper:
                if isinstance(info, bool):
                    return info, "BOOLEAN"
                return str(info).strip().lower() in ("1", "true", "t", "y", "yes"), "BOOLEAN"
            # DATETIME/TIMESTAMP antes de DATE para evitar match parcial
            if any(t in type_upper for t in ("DATETIME", "TIMESTAMP")):
                if isinstance(info, datetime):
                    return info, "DATE"
                if isinstance(info, date):
                    return datetime(info.year, info.month, info.day), "DATE"
                # ISO 8601 / JSON: suporta timezone offset (+00:00) e Z (UTC)
                try:
                    return datetime.fromisoformat(str(info).strip().replace("Z", "+00:00")), "DATE"
                except ValueError:
                    pass
                for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
                    try:
                        return datetime.strptime(str(info).strip(), fmt), "DATE"
                    except ValueError:
                        continue
                return info, "DATE"
            if "DATE" in type_upper:
                if isinstance(info, datetime):
                    return info.date(), "DATE"
                if isinstance(info, date):
                    return info, "DATE"
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
                    try:
                        return datetime.strptime(str(info).strip(), fmt).date(), "DATE"
                    except ValueError:
                        continue
                return info, "DATE"
            if "TIME" in type_upper:
                if isinstance(info, time):
                    return info, "DATE"
                # ISO 8601: HH:MM:SS.ffffff e HH:MM:SS+00:00
                try:
                    return time.fromisoformat(str(info).strip().replace("Z", "+00:00")), "DATE"
                except ValueError:
                    pass
                for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
                    try:
                        return datetime.strptime(str(info).strip(), fmt).time(), "DATE"
                    except ValueError:
                        continue
                return info, "DATE"
            # TEXT
            if any(t in type_upper for t in ("VARCHAR", "CHAR", "TEXT", "CLOB", "STRING", "NCHAR", "NVARCHAR", "UNICODE", "ENUM")):
                return str(info), "TEXT"
            # Outros (BLOB, BINARY, JSON, UUID, ARRAY, etc.)
            return info, "OTHER"
        except Exception:
            return info, "OTHER"

    @staticmethod
    def to_matrix_from_records(column_names: List[str], records: List[List[Any]]) -> List[List[Any]]:
        if not column_names:
            return []
        table_row = ["__result__" for _ in column_names]
        return [table_row, list(column_names), *records]

    @staticmethod
    def to_meta_matrix(rowcount: int) -> List[List[Any]]:
        return [["__meta__"], ["rowcount"], [rowcount]]

    @staticmethod
    def expand_structure(
        columns_definitions: Dict[str, Dict[str, ColumnDefinition]],
        matrix: List[List[Any]],
        include_md: bool = False,
    ) -> Tuple[List[Any], List[Any], List[int]]:
        if not matrix or len(matrix) < 2:
            raise ValueError("Matriz inválida. É necessário ter ao menos 2 linhas.")

        src_tables = list(matrix[0])
        src_headers = list(matrix[1])
        src_pairs = [(src_tables[i], src_headers[i]) for i in range(min(len(src_tables), len(src_headers)))]

        has_md = "MD" in src_headers
        md_idx = src_headers.index("MD") if has_md else -1

        tables_in_order: List[str] = []
        for t in src_tables:
            if t not in tables_in_order and t in columns_definitions:
                tables_in_order.append(t)

        target_tables: List[Any] = []
        target_headers: List[Any] = []
        for t in tables_in_order:
            for col_name in columns_definitions[t].keys():
                target_tables.append(t)
                target_headers.append(col_name)

        if include_md and has_md:
            target_tables.append(src_tables[md_idx])
            target_headers.append("MD")

        source_index_map: List[int] = []
        for i in range(len(target_headers)):
            target_pair = (target_tables[i], target_headers[i])
            idx = next((j for j, pair in enumerate(src_pairs) if pair == target_pair), -1)
            source_index_map.append(idx)

        return target_tables, target_headers, source_index_map

    @staticmethod
    def project_matrix(
        matrix: List[List[Any]],
        target_tables: List[Any],
        target_headers: List[Any],
        source_index_map: List[int],
        default: Any = None,
    ) -> List[List[Any]]:
        if not matrix or len(matrix) < 2:
            raise ValueError("Matriz inválida. É necessário ter ao menos 2 linhas.")

        out: List[List[Any]] = [target_tables, target_headers]
        for src_row in matrix[2:]:
            dst_row: List[Any] = []
            for src_idx in source_index_map:
                if src_idx == -1:
                    dst_row.append(default)
                elif src_idx < len(src_row):
                    dst_row.append(src_row[src_idx])
                else:
                    dst_row.append(default)
            out.append(dst_row)

        return out
