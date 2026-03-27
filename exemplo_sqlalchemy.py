from sqlalchemy import (
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
)

# cria tudo
metadata.create_all(engine)

# ----------------------------
# INSERT
# ----------------------------
with engine.begin() as conn:
    stmt = insert(users).values([
        {"name": "João"},
        {"name": "Maria"},
    ])
    conn.execute(stmt)

# ----------------------------
# SELECT simples
# ----------------------------
with engine.connect() as conn:
    stmt = select(users)
    result = conn.execute(stmt)

    print("\n--- USERS ---")
    for row in result:
        print(row._mapping)  # dict-like

# ----------------------------
# INSERT com FK
# ----------------------------
with engine.begin() as conn:
    stmt = insert(orders).values([
        {"user_id": 1, "product": "Notebook"},
        {"user_id": 1, "product": "Mouse"},
        {"user_id": 2, "product": "Teclado"},
    ])
    conn.execute(stmt)

# ----------------------------
# SELECT com JOIN
# ----------------------------
with engine.connect() as conn:
    stmt = (
        select(
            users.c.name,
            orders.c.product
        )
        .join(orders, users.c.id == orders.c.user_id)
    )

    result = conn.execute(stmt)

    print("\n--- JOIN ---")
    for row in result:
        print(row._mapping)

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