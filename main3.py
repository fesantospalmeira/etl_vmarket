import logging
from modules.getPedido import getDetalhesPedido, getRelatorioPedido
from modules.getToken import getToken
from modules.createConnectionString import create_connection
from modules.SendLogEmail import send_log_email
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
    EMAIL = os.getenv('EMAIL')
    PASS = os.getenv('PASSWORD')
    DRIVER = os.getenv("DRIVER")
    
    baseurl = 'https://integracao-compras.vmarketcompras.com.br/api/'
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
    query_divergentes = r"""
        SELECT DISTINCT id_pedido from vm_tb_divergencias
    """
    
    try:
        with engine.connect() as connection:
            df_pedidos = pd.read_sql(text(query_pedidos), connection)
            df_pedidos_divergentes = pd.read_sql(text(query_divergentes), connection)
        
        lista_pedidos = df_pedidos['id_pedido'].to_list()
        lista_pedidos_divergentes = df_pedidos_divergentes['id_pedido'].to_list()
        
        token_1 = getToken(baseurl,EMAIL,PASS)
        getDetalhesPedido(baseurl,token_1,lista_pedidos,EMAIL,PASS) 
        token_2 = getToken(baseurl,EMAIL,PASS)
        getRelatorioPedido(baseurl,token_2,lista_pedidos_divergentes)
        
        msg = f"✅ Pedidos atualizados com sucesso!"
        print(msg)
        logging.info(msg)
        
    except Exception as e:
        msg = f"❌ Erro ao tentar atualizar pedidos: {e}"
        
        print(msg)
        logging.error(msg)
    
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    TO_EMAIL = os.getenv("TO_EMAIL")

    if EMAIL_USER and EMAIL_PASSWORD and TO_EMAIL:
        send_log_email(
            smtp_server="smtp.gmail.com",
            smtp_port=465,
            email_user=EMAIL_USER,
            email_password=EMAIL_PASSWORD,
            log_file=log_path
        )
    else:
        logging.warning("⚠️ Variáveis de email não configuradas no .env, log não enviado.")
    
if __name__ == '__main__':
    main()
