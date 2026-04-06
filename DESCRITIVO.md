## Objetivo

Biblioteca Python que abstrai consultas, atualizações, inserções e exclusões em qualquer banco de dados suportado pelo SQLAlchemy, utilizando **matrizes bidimensionais** (`List[List[Any]]`) como interface única de entrada e saída.

---

## Modelos de Dados (entrada)

### header

Matriz **2 linhas × N colunas** que define quais colunas serão retornadas em uma pesquisa.

| Linha | Conteúdo                      |
| ----- | ----------------------------- |
| 0     | Nome da tabela de cada coluna |
| 1     | Nome da coluna                |

```python
header = [
    ["users", "orders", "orders"],
    ["name",  "product","status"],
]
```

### filter (opcional)

Matriz **≥ 3 linhas × N colunas** que define critérios de filtragem (cláusula `WHERE`).

| Linha | Conteúdo                              |
| ----- | ------------------------------------- |
| 0     | Nome da tabela de cada coluna         |
| 1     | Nome da coluna                        |
| 2+    | Valores de filtro (`None` = ignorado) |

- **Entre colunas** (mesma linha): condição `AND`.
- **Entre linhas** (linhas distintas): condição `OR`.
- Suporta operadores via tupla: `("!=", valor)`, `(">", valor)`, `(">=", valor)`, `("<", valor)`, `("<=", valor)`, `("like", valor)`.

```python
# status = "OPEN" AND user_id = 1   OR   status = "CLOSED"
filter = [
    ["orders",  "orders" ],
    ["status",  "user_id"],
    ["OPEN",    1        ],
    ["CLOSED",  None     ],
]
```

### data

Matriz **≥ 3 linhas × N colunas** usada em operações de escrita (insert/update/delete).

| Linha | Conteúdo                                                   |
| ----- | ---------------------------------------------------------- |
| 0     | Nome da tabela de cada coluna                              |
| 1     | Nome da coluna (**última coluna obrigatoriamente `"MD"`**) |
| 2+    | Valores + marcador na coluna `MD`                          |

Valores aceitos na coluna `MD`:

- `"U"` / `"A"` → Atualização (se PK existir e registro encontrado) ou Inserção (caso contrário).
- `"D"` → Remoção (exige PK presente no data).

```python
data = [
    ["users", "users", "users", "users"],
    ["id",    "name",  "email", "MD"   ],
    [1,       "Ana",   "a@x.com","U"   ],
    [5,       "Novo",  "n@x.com","U"   ],
    [3,       None,    None,     "D"   ],
]
```

### relationships (opcional)

Lista de listas que define JOINs entre tabelas. Leitura de baixo para cima; tabela A complementa tabela B.

| Índice  | Campo    | Descrição                                  |
| ------- | -------- | ------------------------------------------ |
| 0       | Tabela A | Nome da tabela origem do JOIN              |
| 1       | Tabela B | Nome da tabela destino do JOIN             |
| 2       | Coluna A | Coluna de Tabela A usada na condição       |
| 3       | Coluna B | Coluna de Tabela B usada na condição       |
| 4 (opc) | inner    | `1` (padrão) = INNER JOIN, `0` = LEFT JOIN |

```python
relationships = [
    ["orders", "users", "user_id", "id", 1],
]
```

---

## Modelos de Dados (saída)

Toda saída é uma **matriz bidimensional** com a mesma estrutura:

| Linha | Conteúdo                                                         |
| ----- | ---------------------------------------------------------------- |
| 0     | Nome da tabela de cada coluna (ou `"__result__"` / `"__meta__"`) |
| 1     | Nome da coluna                                                   |
| 2+    | Registros retornados                                             |

Quando `complete=True`, a saída é expandida para incluir **todas** as colunas das tabelas envolvidas (preenchendo com `default` as ausentes).

Para `execute()` / `execute_stmt()` sem linhas de retorno, a saída é `[["__meta__"], ["rowcount"], [n]]`.

---

## Estruturas internas

### \_columns_definitions

Dicionário gerado a partir de `MetaData`:

```
tabela → coluna → {
    "type":       <SQLAlchemy Type>,
    "primary":    bool,
    "unique":     bool,
    "default":    valor | None,
    "nullable":   bool,
    "table_obj":  <Table>,
    "column_obj": <Column>,
}
```

### \_primary_keys

Dicionário `tabela → nome_coluna_pk` (`None` se não houver PK).

---

## Classes

### DbDriver

Classe principal — ponto de entrada da biblioteca.

```python
DbDriver(metadata: MetaData, engine: Engine)
```

| Atributo    | Tipo             | Descrição                 |
| ----------- | ---------------- | ------------------------- |
| `Pesquisar` | `DbDriverSearch` | Sub-objeto para consultas |
| `Atualizar` | `DbDriverUpdate` | Sub-objeto para escrita   |

| Método                | Entrada              | Saída                                                   |
| --------------------- | -------------------- | ------------------------------------------------------- |
| `execute(query: str)` | SQL puro como string | Matriz resultado ou `[["__meta__"], ["rowcount"], [n]]` |
| `execute_stmt(stmt)`  | Statement SQLAlchemy | Idem                                                    |

---

### DbDriverCore (classe base)

```python
DbDriverCore(metadata: MetaData, engine: Engine)
```

| Método                                | Entrada             | Saída  | Descrição                                                     |
| ------------------------------------- | ------------------- | ------ | ------------------------------------------------------------- |
| `reset()`                             | —                   | —      | Remove todos atributos públicos (não prefixados com `_`)      |
| `define_filter(filter)`               | Matriz filter       | `self` | Valida colunas e salva `self.filter_positions`, `self.filter` |
| `define_relationships(relationships)` | Lista relationships | `self` | Valida tabelas/colunas e salva `self.relationships`           |

---

### DbDriverSearch (herda DbDriverCore)

| Método                                             | Entrada       | Saída            | Descrição                                                           |
| -------------------------------------------------- | ------------- | ---------------- | ------------------------------------------------------------------- |
| `reset()`                                          | —             | —                | Remove `table`, `header`, `filter`                                  |
| `define_header(header)`                            | Matriz header | `self`           | Valida e salva `self.header_positions`, `self.header`, `self.table` |
| `search(reset=True, complete=False, default=None)` | —             | Matriz resultado | Executa SELECT; `complete=True` expande todas colunas das tabelas   |

---

### DbDriverUpdate (herda DbDriverCore)

| Método                                             | Entrada     | Saída       | Descrição                                                                              |
| -------------------------------------------------- | ----------- | ----------- | -------------------------------------------------------------------------------------- |
| `define_data(data)`                                | Matriz data | `self`      | Valida colunas (incluindo MD) e salva `self.data_positions`, `self.data`               |
| `update(reset=True, complete=False, default=None)` | —           | Matriz data | Executa U/A (upsert) e D (delete) linha a linha; `complete=True` expande todas colunas |

Regras de `update()`:

- `"U"` / `"A"`: se PK presente e registro existe → UPDATE; senão → INSERT (valida colunas obrigatórias).
- `"D"`: exige PK; executa DELETE.
- Filtro extra (via `define_filter`) é adicionado como `WHERE` adicional.

---

### DbDriverUtils (classe estática, não instanciável)

| Método                                                                             | Entrada                    | Saída                                                               |
| ---------------------------------------------------------------------------------- | -------------------------- | ------------------------------------------------------------------- |
| `get_columns_definitions(metadata)`                                                | `MetaData`                 | `Dict[str, Dict[str, Dict[str, Any]]]`                              |
| `get_primary_keys(metadata)`                                                       | `MetaData`                 | `Dict[str, str \| None]`                                            |
| `get_valid_columns(columns_definitions, matrix)`                                   | Dict + Matriz (2 linhas)   | `(List[int], List[List[Any]])` — posições válidas e matriz filtrada |
| `is_valid_table(columns_definitions, table)`                                       | Dict + str                 | `bool`                                                              |
| `buid_select(columns_definitions, headers, relationships, filters)`                | Dict + matrizes            | Statement `SELECT` do SQLAlchemy                                    |
| `buid_update(columns_definitions, data, relationships, filters)`                   | Dict + matrizes            | `List[Statement]` — lista de `UPDATE`                               |
| `buid_insert(columns_definitions, data)`                                           | Dict + matriz data         | Statement `INSERT` (ou `None`)                                      |
| `buid_delete(columns_definitions, keys, relationships, filters)`                   | Dict + matrizes            | `List[Statement]` — lista de `DELETE`                               |
| `_build_filters(columns_definitions, filters)`                                     | Dict + matriz filter       | Expressão `WHERE` SQLAlchemy (ou `None`)                            |
| `_valid_info(type, info)`                                                          | str tipo + valor           | Valor convertido para o tipo da coluna                              |
| `to_matrix_from_records(column_names, records)`                                    | `List[str]` + `List[List]` | Matriz com `"__result__"` na linha de tabelas                       |
| `to_meta_matrix(rowcount)`                                                         | `int`                      | `[["__meta__"], ["rowcount"], [n]]`                                 |
| `expand_structure(columns_definitions, matrix, include_md)`                        | Dict + matriz + bool       | `(target_tables, target_headers, source_index_map)`                 |
| `project_matrix(matrix, target_tables, target_headers, source_index_map, default)` | Matrizes + mapa            | Matriz expandida com todas colunas                                  |
