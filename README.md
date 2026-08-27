# pyEasyMatrixDb

Biblioteca Python para consultar e modificar bancos de dados usando **matrizes bidimensionais** como interface.  
Abstrai SELECT, INSERT, UPDATE e DELETE via SQLAlchemy Core — funciona com qualquer banco suportado (SQLite, PostgreSQL, MySQL, etc.).

## Instalação

```bash
pip install pyeasymatrixdb
```

## GitHub

https://github.com/RicNazar/toxt-p10_dbdriver

## Início Rápido

```python
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey

# Cria uma engine em memória
engine = create_engine("sqlite+pysqlite:///:memory:")
metadata = MetaData()

users = Table("users", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(100), nullable=False),
    Column("email", String(150), nullable=False),
)

orders = Table("orders", metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", ForeignKey("users.id"), nullable=False),
    Column("product", String(100), nullable=False),
    Column("status", String(20), nullable=False),
)

# Cria as tabelas caso não existam
metadata.create_all(engine)

from pyeasymatrixdb import DbDriver
connection = engine.connect()
db = DbDriver(metadata, connection)
```

## Funcionalidades

### 1. SQL puro — `execute()` / `execute_stmt()`

```python
# SQL como texto
resultado = db.execute("SELECT id, name FROM users ORDER BY id")
# → [["__result__", "__result__"], ["id", "name"], [1, "Ana"], [2, "Bruno"]]

# Statement SQLAlchemy
from sqlalchemy import select
stmt = select(users.c.id, users.c.name).where(users.c.id >= 2)
resultado = db.execute_stmt(stmt)
```

Comandos sem retorno de linhas devolvem `[["__meta__"], ["rowcount"], [n]]`.

---

### 2. Pesquisa — `db.Pesquisar`

#### Pesquisa simples

```python
resultado = (
    db.Pesquisar
    .define_header([
        ["users", "users"],
        ["id",    "name"],
    ])
    .search()
)
# → [["users", "users"], ["id", "name"], [1, "Ana"], [2, "Bruno"], ...]
```

#### Pesquisa com JOIN + filtro

```python
resultado = (
    db.Pesquisar
    .define_header([
        ["users",  "orders",  "orders"],
        ["name",   "product", "status"],
    ])
    .define_relationships([
        ["orders", "users", "user_id", "id", 1],  # INNER JOIN
    ])
    .define_filter([
        ["orders"],
        ["status"],
        ["OPEN"],
    ])
    .search()
)
```

#### Filtro com OR (múltiplas linhas)

```python
resultado = (
    db.Pesquisar
    .define_header([["users", "users"], ["id", "name"]])
    .define_filter([
        ["users"],
        ["name"],
        ["Ana"],     # OR
        ["Carla"],
    ])
    .search()
)
```

Na matriz de filtros, valores na mesma linha normalmente são combinados com `AND`. Linhas diferentes continuam sendo alternativas completas combinadas com `OR`.

Quando a mesma combinação de tabela e coluna aparece mais de uma vez na mesma linha, os valores dessa coluna são agrupados com `OR` antes de combinar com as demais colunas:

```python
.define_filter([
    ["orders", "orders", "orders"],
    ["id",     "id",     "status"],
    ["10",     "20",     "OPEN"],
])
# equivale a: (orders.id = 10 OR orders.id = 20) AND orders.status = 'OPEN'
```

O agrupamento considera a chave completa `(tabela, coluna)`. Colunas com o mesmo nome em tabelas diferentes são agrupadas separadamente.

#### Filtro com operadores

```python
.define_filter([
    ["users", "users", "users"],
    ["id",    "name",  "name" ],
    [">=2",   "!=Ana", "*bru*"],  # >, >=, <, <=, !=, == e curingas com *
])
```

#### Filtro para valores SQL NULL

Use `null` como palavra reservada de filtro para gerar operadores SQL nativos de nulo, em qualquer tipo de coluna:

| Sintaxe  | SQLAlchemy gerado     |
| -------- | --------------------- |
| `null`   | `column.is_(None)`    |
| `=null`  | `column.is_(None)`    |
| `==null` | `column.is_(None)`    |
| `!=null` | `column.is_not(None)` |

O reconhecimento ignora espaços e maiúsculas/minúsculas, por exemplo `" NULL "`, `"==NULL"` e `"!= Null"`.

Compatibilidade: por essa convenção, o texto `"null"` no filtro passa a significar SQL `NULL` também em colunas de texto. A sintaxe atual de filtros não possui escape explícito para buscar a string literal `"null"`; use outra condição SQLAlchemy via API de statement quando precisar distinguir esse texto literal de um valor nulo.

#### Formatos aceitos em `define_filter(...)`

- Valor literal: `"OPEN"`, `10`, `"2026-01-01"` (comparação por igualdade)
- Operador inline em string: `">=2"`, `"<10"`, `"!=OPEN"`, `"==Ana"`
- Valor nulo SQL: `"null"`, `"=null"`, `"==null"`, `"!=null"`
- Texto com curinga `*`: `"Ana*"`, `"*silva"`, `"*ana*"` (vira `LIKE`)

Observações importantes:

- Tuplas como `[(">=", 2)]` não são interpretadas como operador + valor no código atual.
- Para colunas de texto, `"=="` vazio (`"=="`) gera condição para vazio/nulo.
- Valores "falsy" literais no filtro (ex.: `0`, `False`, `""`, `None`) são ignorados pelo parser de filtro atual.

#### Pesquisa completa (todas as colunas)

```python
resultado = db.Pesquisar.define_header(header).search(complete=True, default=None)
# Expande a saída para todas as colunas das tabelas envolvidas.
# Colunas ausentes são preenchidas com o valor de `default`.
```

---

### 3. Atualização — `db.Atualizar`

A coluna `MD` (última) controla a operação por linha:

- `"U"` / `"A"` → upsert
- `"D"` → delete

`update()` retorna uma lista simples com os IDs afetados ou inseridos.

#### Upsert (update + insert)

```python
resultado = (
    db.Atualizar
    .define_data([
        ["users", "users", "users",     "users"],
        ["id",    "name",  "email",     "MD"   ],
        [1,       "Ana R.","ana@x.com", "U"    ],  # atualiza id=1
        [5,       "Novo",  "novo@x.com","U"    ],  # insere id=5
    ])
    .update()
)

# → [1, 5]
```

#### Delete

```python
resultado = (
    db.Atualizar
    .define_data([
        ["orders", "orders",  "orders"],
        ["id",     "product", "MD"    ],
        [101,      "Mouse",   "D"    ],
    ])
    .update()
)

# → []
```

#### Atualização com filtro extra

```python
resultado = (
    db.Atualizar
    .define_data([
        ["orders", "orders", "orders",  "orders"],
        ["id",     "user_id","status",  "MD"    ],
        [100,      1,        "CLOSED",  "U"     ],
    ])
    .define_filter([
        ["orders"],
        ["status"],
        ["OPEN"],       # só atualiza se status = "OPEN"
    ])
    .update()
)

# → [100]
```

#### Batch automático

```python
resultado = db.Atualizar.define_data(data).update()
```

Quando há mais de 20 linhas e todas usam `"U"` ou `"A"` com PK presente no `data`, o driver agrupa updates e inserts em lote automaticamente.

Para evitar limites grandes de parâmetros no banco, a leitura prévia das PKs existentes é feita em blocos de até 900 IDs.

---

### 4. Encadeamento (fluent API)

Todos os métodos `define_*` retornam `self`, permitindo encadeamento:

```python
db.Pesquisar.define_header(h).define_relationships(r).define_filter(f).search()
db.Atualizar.define_data(d).define_filter(f).update()
```

### 5. Reset automático

Por padrão, `search()` e `update()` executam `reset()` após a operação, limpando header/filter/data para a próxima chamada. Passe `reset=False` para manter o estado.

---

## Formato das Matrizes

| Modelo            | Linhas | Descrição                                                             |
| ----------------- | ------ | --------------------------------------------------------------------- |
| **header**        | 2      | `[tabelas, colunas]` — define colunas do SELECT                       |
| **filter**        | ≥ 3    | `[tabelas, colunas, valores...]` — AND entre colunas, OR entre linhas |
| **data**          | ≥ 3    | `[tabelas, colunas, valores...]` — última coluna = `"MD"`             |
| **relationships** | N      | `[[tabelaA, tabelaB, colA, colB, inner?], ...]`                       |

## Estrutura do Projeto

```
app/
  DbDriver/
    DbDriver.py        — Classe principal (execute, execute_stmt)
    subclasses/
      DbDriverCore.py   — Classe base (reset, define_filter, define_relationships)
      DbDriverSearch.py — Pesquisa (define_header, search)
      DbDriverUpdate.py — Escrita (define_data, update)
      DbDriverUtils.py  — Utilitários estáticos (builders, validações)
```

## Licença

Consulte o arquivo de licença do repositório.
