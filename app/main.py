from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, create_engine, select

from DbDriver import DbDriver


def print_block(title: str, rows):
	print(f"\n=== {title} ===")
	if not rows:
		print("(sem dados)")
		return

	# Normaliza tudo para string para calcular largura de cada coluna.
	str_rows = [["null" if cell is None else str(cell) for cell in row] for row in rows]
	col_count = max(len(row) for row in str_rows)

	for row in str_rows:
		if len(row) < col_count:
			row.extend(["null"] * (col_count - len(row)))

	widths = [max(len(row[col]) for row in str_rows) for col in range(col_count)]

	for i, row in enumerate(str_rows):
		line = " | ".join(row[col].ljust(widths[col]) for col in range(col_count))
		print(line)
		if i == 1 and len(str_rows) > 2:
			sep = "-+-".join("-" * widths[col] for col in range(col_count))
			print(sep)


def build_schema(metadata: MetaData):
	users = Table(
		"users",
		metadata,
		Column("id", Integer, primary_key=True),
		Column("name", String(100), nullable=False),
		Column("email", String(150), unique=True, nullable=False),
	)

	orders = Table(
		"orders",
		metadata,
		Column("id", Integer, primary_key=True),
		Column("user_id", ForeignKey("users.id"), nullable=False),
		Column("product", String(100), nullable=False),
		Column("status", String(20), nullable=False, default="OPEN"),
	)

	return users, orders


def seed_data(driver: DbDriver):
	driver.execute(
		"""
		INSERT INTO users (id, name, email) VALUES
		(1, 'Ana', 'ana@acme.com'),
		(2, 'Bruno', 'bruno@acme.com'),
		(3, 'Carla', 'carla@acme.com')
		"""
	)

	driver.execute(
		"""
		INSERT INTO orders (id, user_id, product, status) VALUES
		(100, 1, 'Notebook', 'OPEN'),
		(101, 1, 'Mouse', 'OPEN'),
		(102, 2, 'Teclado', 'OPEN')
		"""
	)


def demo_execute(driver: DbDriver, users: Table):
	rows = driver.execute("SELECT id, name, email FROM users ORDER BY id")
	print_block("execute(query): SELECT users", rows)

	rows_stmt = driver.execute_stmt(select(users.c.id, users.c.name).where(users.c.id >= 2))
	print_block("execute_stmt(stmt): users id >= 2", rows_stmt)


def demo_search(driver: DbDriver):
	header_join = [
		["users", "orders", "orders"],
		["name", "product", "status"],
	]

	relationships = [
		["orders", "users", "user_id", "id", 1],
	]

	filters_open = [
		["orders"],
		["status"],
		["OPEN"],
	]

	rows_join = (
		driver.Pesquisar
		.define_header(header_join)
		.define_relationships(relationships)
		.define_filter(filters_open)
		.search(reset=True)
	)
	print_block("Pesquisar.search com header + relationships + filter", rows_join)

	rows_join_complete = (
		driver.Pesquisar
		.define_header(header_join)
		.define_relationships(relationships)
		.define_filter(filters_open)
		.search(reset=True, complete=True, default=None)
	)
	print_block("Pesquisar.search complete=True (default=None)", rows_join_complete)

	# Exemplo com OR entre linhas: name = Ana OR name = Carla
	header_users = [
		["users", "users"],
		["id", "name"],
	]
	filters_or = [
		["users"],
		["name"],
		["Ana"],
		["Carla"],
	]

	rows_or = driver.Pesquisar.define_header(header_users).define_filter(filters_or).search(reset=True)
	print_block("Pesquisar.search com filtro OR (duas linhas)", rows_or)


def demo_update(driver: DbDriver):
	# data: primeira linha tabela, segunda headers, últimas linhas dados + MD (U/A/D)
	update_data = [
		["users", "users", "users", "users"],
		["id", "name", "email", "MD"],
		[1, "Ana Souza", "ana@acme.com", "U"],
		[4, "Diego", "diego@acme.com", "U"],
	]

	result_upsert = driver.Atualizar.define_data(update_data).update(reset=True)
	print_block("Atualizar.update retorno no formato de data", result_upsert)

	result_upsert_complete = (
		driver.Atualizar
		.define_data(update_data)
		.update(reset=True, complete=True, default=None)
	)
	print_block("Atualizar.update complete=True (default=None)", result_upsert_complete)

	delete_data = [
		["orders", "orders", "orders", "orders"],
		["id", "user_id", "product", "MD"],
		[101, 1, "Mouse", "D"],
	]

	result_delete = driver.Atualizar.define_data(delete_data).update(reset=True)
	print_block("Atualizar.update com MD=D", result_delete)

	# Filtro extra aplicado ao update/delete (where adicional)
	guarded_update_data = [
		["orders", "orders", "orders", "orders"],
		["id", "user_id", "status", "MD"],
		[100, 1, "CLOSED", "U"],
	]
	guarded_filter = [
		["orders"],
		["status"],
		["OPEN"],
	]

	result_guarded = (
		driver.Atualizar
		.define_data(guarded_update_data)
		.define_filter(guarded_filter)
		.update(reset=True)
	)
	print_block("Atualizar.update com filtro extra", result_guarded)

	result_guarded_complete_custom = (
		driver.Atualizar
		.define_data(guarded_update_data)
		.define_filter(guarded_filter)
		.update(reset=True, complete=True, default="<vazio>")
	)
	print_block("Atualizar.update complete=True (default='<vazio>')", result_guarded_complete_custom)


def show_final_state(driver: DbDriver):
	users_rows = driver.execute("SELECT id, name, email FROM users ORDER BY id")
	orders_rows = driver.execute("SELECT id, user_id, product, status FROM orders ORDER BY id")
	print_block("Estado final users", users_rows)
	print_block("Estado final orders", orders_rows)


def main():
	engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
	metadata = MetaData()
	users, _ = build_schema(metadata)
	metadata.create_all(engine)

	driver = DbDriver(metadata, engine)

	seed_data(driver)
	demo_execute(driver, users)
	demo_search(driver)
	demo_update(driver)
	show_final_state(driver)


if __name__ == "__main__":
	main()
