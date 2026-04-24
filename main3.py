import logging
from modules.getPedido import getDetalhesPedido, getRelatorioPedido
from modules.createConnectionString import create_connection
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import text
import os
import pandas as pd

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(BASE_DIR, "app_pedidos_reprocess.log")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

def main():
    DB_SERVER = os.getenv("DB_SERVER")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DRIVER = os.getenv("DRIVER")
    odbc_params = quote_plus(
                f"DRIVER={{{DRIVER}}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_DATABASE};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"  
            )
    engine = create_connection(odbc_params)
    query_pedidos = r"""
        SELECT DISTINCT 
            id_pedido 
        FROM vm_tb_pedidos 
        WHERE DT_INCLUSAO >= DATEADD(DAY, -60, SYSDATETIME())
        """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query_pedidos), connection)
        print(df.head(5))
    except Exception as e:
        print(f"Erro: {e}")
    
if __name__ == '__main__':
    main()
