from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    text,
    Identity
)
#from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

tab_pedidos = Table(
    "tab_pedidos",
    metadata,
    Column("id_pedido_bm", Integer, primary_key=True,autoincrement=False, comment="Identificador do pedido BM."),
    Column("status", String(20), nullable=False, comment="Status atual do pedido."),
    Column("data_pedido", Date, nullable=False, comment="Data em que o pedido foi registrado."),
    Column("planta", String(50), nullable=False, comment="Planta associada ao pedido."),
    Column("cliente", String(250), nullable=False, comment="Cliente vinculada ao pedido."),
    Column("pedido_cliente", String(250), nullable=False, comment="Identificação do pedido no cliente."),
    Column("destino", String(120), nullable=False, comment="Destino do embarque."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)

tab_pedidos_itens = Table(
    "tab_pedidos_itens",
    metadata,
    Column("id_pedido_item", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do item do pedido."),
    Column("id_pedido_bm", Integer, ForeignKey("tab_pedidos.id_pedido_bm"), nullable=False, comment="Identificador do pedido BM."),
    Column("id_produto", Integer, ForeignKey("tab_produtos.id_produto"), nullable=False, comment="Identificador do produto."),
    Column("bitola", String(20), nullable=False, comment="Bitola do material."),
    Column("metragem", Numeric(10,3), nullable=False, comment="Metragem total do produto no booking."),
    Column("pecas", Integer, nullable=False, comment="Quantidade total de peças."),
    Column("preco", Numeric(10,6), nullable=False, comment="Preço unitário do produto."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)   

tab_pedidos_itens_agendas = Table(
    "tab_pedidos_itens_agendas",
    metadata,
    Column("id_pedido_item_agenda", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do agendamento do item do pedido."),
    Column("id_pedido_item", Integer, ForeignKey("tab_pedidos_itens.id_pedido_item"), nullable=False, comment="Identificador do item do pedido."),
    Column("data_agendada", Date, nullable=False, comment="Data agendada para o item do pedido."),
    Column("Booking", String(120), nullable=False, comment="Número do booking associado ao agendamento."),
    Column("status", String(20), nullable=False, comment="Status do agendamento."),
    Column("metragem", Numeric(10,3), nullable=False, comment="Metragem total do produto no booking."),
    Column("pecas", Integer, nullable=False, comment="Quantidade total de peças."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
    CheckConstraint("status IN ('Previsto', 'Planejado', 'Embarcado')", name="ck_tab_pedidos_itens_agendas_status")
)

tab_produtos = Table(
    "tab_produtos",
    metadata,
    Column("id_produto", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do produto."),
    Column("codigo", String(100), nullable=False, unique=True, comment="Código principal do produto."),
    Column("ordem", Integer, nullable=False, comment="Ordem de exibição ou classificação do produto."),
    Column("codigos_equivalentes", String(100), nullable=False, comment="Lista de códigos separados por ; equivalentes do produto."),
    Column("pecas_fardo", Integer, nullable=False, server_default=text("0"), comment="Quantidade de peças por fardo."),
    Column("metragem_peca", Numeric, nullable=False, server_default=text("0"), comment="Metragem de cada peça."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)

tab_bookings = Table(
    "tab_bookings",
    metadata,
    Column("id_booking", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do booking."),
    Column("booking", String(120), nullable=False, comment="Número do booking."),
    Column("id_pedido_bm", Integer, nullable=False, comment="Identificador do pedido BM relacionado."),
    Column("planta", String(50), nullable=False, comment="Planta responsável pelo booking."),
    Column("cliente", String(250), nullable=False, comment="Cliente associado ao booking."),
    Column("destino", String(120), nullable=False, comment="Destino do embarque."),
    Column("armador", String(120), nullable=False, comment="Nome do armador."),
    Column("fatura", String(120), nullable=False, comment="Número da fatura."),
    Column("data_saida", Date, nullable=False, comment="Data de saída do embarque."),
    Column("data_entrega_estimada", Date, nullable=False, comment="Data estimada de entrega."),
    Column("data_entrega_efetiva", Date, nullable=True, comment="Data efetiva de entrega, quando finalizada."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
    Column("log_efetiva_id_usuario", Integer, nullable=True, comment="Identificador do usuário que registrou a entrega efetiva."),
    Column("log_efetiva_timestamp", DateTime(timezone=True), nullable=True, comment="Data e hora em que a entrega efetiva foi registrada."),
    UniqueConstraint("booking", name="uq_tab_bookings_booking"),
)

tab_booking_produtos = Table(
    "tab_booking_produtos",
    metadata,
    Column("id_booking_produto", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do vínculo booking-produto."),
    Column("id_produto", ForeignKey("tab_produtos.id_produto", ondelete="RESTRICT"), nullable=False, comment="Produto relacionado ao booking."),
    Column("id_booking", ForeignKey("tab_bookings.id_booking", ondelete="CASCADE"), nullable=False, comment="Booking relacionado ao produto."),
    Column("bitola", String(20), nullable=False, comment="Bitola do material."),
    Column("fardos", Integer, nullable=False, comment="Quantidade de fardos para o produto."),
    Column("metragem", Numeric(10,3), nullable=False, comment="Metragem total do produto no booking."),
    Column("pecas", Integer, nullable=False, comment="Quantidade total de peças."),
    Column("preco_pecas", Numeric(10,3), nullable=False, comment="Preço por peça."),
    Column("data_produto_utilizado", Date, nullable=True, comment="Data em que o produto foi utilizado."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)

tab_baixas_estoque = Table(
    "tab_baixas_estoque",
    metadata,
    Column("id_baixa_estoque", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador da baixa de estoque."),
    Column("data_baixa", Date, nullable=False, server_default=text("CURRENT_DATE"), comment="Data da baixa de estoque."),
    Column("observacao", String(255), nullable=True, comment="Observações sobre a baixa."),
    Column("cliente", String(250), nullable=False, comment="Cliente relacionado à baixa."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
    Column("log_baixa_id_usuario", Integer, nullable=True, comment="Identificador do usuário que registrou a baixa."),
    Column("log_baixa_timestamp", DateTime(timezone=True), nullable=True, comment="Data e hora em que a baixa foi registrada."),
)

tab_baixas_estoque_produto = Table(
    "tab_baixas_estoque_produto",
    metadata,
    Column("id_baixa_estoque_produto", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador da baixa de produto em estoque."),
    Column("id_baixa_estoque", Integer, nullable=False, comment="Baixa de estoque relacionada."),
    Column("id_booking_produto", ForeignKey("tab_booking_produtos.id_booking_produto", ondelete="RESTRICT"), nullable=False, comment="Item de booking associado à baixa."),
    Column("id_produto", ForeignKey("tab_produtos.id_produto", ondelete="RESTRICT"), nullable=False, comment="Produto baixado do estoque."),
    Column("pecas", Integer, nullable=False, comment="Quantidade de peças baixadas."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)

tab_solicitacoes = Table(
    "tab_solicitacoes",
    metadata,
    Column("id_solicitacao", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador da solicitação."),
    Column("data_solicitacao", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da solicitação."),
    Column("status", String(20), nullable=False, server_default=text("'aberta'"), comment="Status atual da solicitação."),
    Column("cliente", String(250), nullable=False, comment="Cliente relacionado à solicitação."),
    Column("planta", String(50), nullable=False, comment="Planta associada à solicitação."),
    Column("observacao", String(255), nullable=True, comment="Observação adicional da solicitação."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
    Column("log_consolidada_id_usuario", Integer, nullable=True, comment="Identificador do usuário que consolidou a solicitação."),
    Column("log_consolidada_timestamp", DateTime(timezone=True), nullable=True, comment="Data e hora em que a solicitação foi consolidada."),
    Column("log_cancelada_id_usuario", Integer, nullable=True, comment="Identificador do usuário que cancelou a solicitação."),
    Column("log_cancelada_timestamp", DateTime(timezone=True), nullable=True, comment="Data e hora em que a solicitação foi cancelada."),
    CheckConstraint("status IN ('aberta', 'consolidada', 'cancelada')", name="ck_tab_solicitacoes_status"),
)

tab_solicitacoes_itens = Table(
    "tab_solicitacoes_itens",
    metadata,
    Column("id_solicitacao_item", Integer, Identity(), autoincrement=True, primary_key=True, comment="Identificador do item da solicitação."),
    Column("id_solicitacao", ForeignKey("tab_solicitacoes.id_solicitacao", ondelete="CASCADE"), nullable=False, comment="Solicitação principal do item."),
    Column("id_produto", ForeignKey("tab_produtos.id_produto", ondelete="RESTRICT"), nullable=False, comment="Produto solicitado."),
    Column("pecas", Integer, nullable=False, comment="Quantidade de peças solicitadas."),
    Column("observacao", String(255), nullable=True, comment="Observação específica do item."),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora de criação do registro."),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="Data e hora da última atualização do registro."),
)