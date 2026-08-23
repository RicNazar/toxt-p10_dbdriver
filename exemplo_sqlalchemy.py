from collections.abc import Callable
from src.pyeasymatrixdb.DbDriver import DbDriver
from sqlalchemy import (
    Connection,
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    select,
    insert,
    update,
    delete
)

# ----------------------------
# CONFIG
# ----------------------------
DATABASE_URL = "sqlite+pysqlite:///:memory:"  # pode trocar pra postgres depois

engine = create_engine(DATABASE_URL, echo=True)
metadata = MetaData()

# ----------------------------
# TABLES (CORE)
# ----------------------------
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(100), nullable=False),
)

orders = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", ForeignKey("users.id")),
    Column("product", String(100)),
    Column("quantity", Integer, default=0, nullable=False)
)

# cria tudo
metadata.create_all(engine)
get_db_driver: Callable[[Connection], DbDriver] = lambda conn: DbDriver(metadata, conn)

# ----------------------------
# INSERT
# ----------------------------
returned = []
with engine.begin() as conn:
    db_driver = get_db_driver(conn)
    data = [
        [
            "users","users","users"
        ],
        [
            "id","name","MD"
        ],
        [
            None,"João","A"
        ],
        [
            None,"Maria","A"
        ],
    ]
    returned = db_driver.Atualizar.define_data(data).update()
print(f"\n--- ATUALIZAR users --- returned:{returned}")

# ----------------------------
# UPSERT
# ----------------------------
with engine.begin() as conn:
    db_driver = get_db_driver(conn)
    data = [
        [
            "users","users","users"
        ],
        [
            "id","name","MD"
        ],
        [
            2,"Maria Silva Sauro","A"
        ],
        [
            None,"Ricardo Novo","A"
        ],
        [
            5,"Ricardo 5","A"
        ],
    ]
    returned = db_driver.Atualizar.define_data(data).update()
print(f"\n--- UPSERT users --- returned:{returned}")


# ----------------------------
# SELECT simples
# ----------------------------
with engine.begin() as conn:
    db_driver = get_db_driver(conn)
    data = [
        [
            "users","users"
        ],
        [
            "id","name"
        ],
    ]
    returned = db_driver.Pesquisar.define_header(data).search()
print(f"\n--- PESQUISAR users --- returned:\n{returned}\n")

# ----------------------------
# SELECT com JOIN
# ----------------------------
with engine.begin() as conn:
    db_driver = get_db_driver(conn)
    data = [
        [
            "users","users","orders","orders","orders"
        ],
        [
            "id","name","id","product","quantity"
        ],
    ]
    relationships = [
        [
            "users","orders","id","user_id"
        ]
    ]
    returned = db_driver.Pesquisar.define_header(data).define_relationships(relationships).search()
print(f"\n--- PESQUISAR users --- returned:\n{returned}\n")

# ----------------------------
# UPDATE
# ----------------------------
with engine.begin() as conn:
    stmt = (
        update(users)
        .where(users.c.name == "João")
        .values(name="João Silva")
    )
    conn.execute(stmt)

# ----------------------------
# DELETE
# ----------------------------
with engine.begin() as conn:
    stmt = delete(orders).where(orders.c.product == "Mouse")
    conn.execute(stmt)

# ----------------------------
# SELECT final
# ----------------------------
with engine.connect() as conn:
    stmt = select(users)
    result = conn.execute(stmt)

    print("\n--- USERS FINAL ---")
    for row in result:
        print(row._mapping)