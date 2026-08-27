from metadata import metadata
from sqlalchemy import create_engine
from src.pyeasymatrixdb.DbDriver import DbDriver

DATABASE_URL = "postgresql+psycopg://postgres:rj74wj3htdz563hwrr6v@179.197.79.251:7514/vmi"
engine = create_engine(DATABASE_URL, echo=False)

with engine.begin() as conn:
    driver = DbDriver(metadata, conn)
    headers = [
    ["tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_bookings","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_booking_produtos","tab_baixas_estoque_produto","tab_baixas_estoque_produto","tab_baixas_estoque","tab_baixas_estoque","tab_baixas_estoque","tab_baixas_estoque","tab_baixas_estoque"],
        ["id_booking","booking","id_pedido_bm","planta","cliente","destino","armador","fatura","data_saida","data_entrega_estimada","data_entrega_efetiva","log_efetiva_id_usuario","log_efetiva_timestamp","id_booking_produto","bitola","metragem","preco_pecas","id_produto","pecas","fardos","data_produto_utilizado","preco_pecas","id_baixa_estoque_produto","pecas","id_baixa_estoque","data_baixa","observacao","log_baixa_id_usuario","log_baixa_timestamp"]
        ]
    relationships =[
        ["tab_baixas_estoque","tab_baixas_estoque_produto","id_baixa_estoque","id_baixa_estoque"],
        ["tab_baixas_estoque_produto","tab_booking_produtos","id_booking_produto","id_booking_produto"],
        ["tab_bookings","tab_booking_produtos","id_booking","id_booking"]]
    linhas = driver.Pesquisar.define_header(headers).define_relationships(relationships).search()

    print("linhas", linhas)