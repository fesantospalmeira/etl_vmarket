import logging
import requests as r
from modules.getToken import getToken 
from modules.getFiliais import getData as getFiliais
from modules import (
    getFornecedor as forn,
    getCotacao as cot,
    getPreCotacao as precot,
    getProdutos as prod,
    getSecao as sec,
    getPedido as ped,
    SendLogEmailPed as eml
    
)
from dotenv import load_dotenv
import os

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(BASE_DIR, "app_pedidos.log")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

def main():
    email = os.getenv('EMAIL')
    password = os.getenv('PASSWORD')
    base_url = 'https://integracao-compras.vmarketcompras.com.br/api/'
    token = getToken(base_url,email,password)
    lista_filiais = getFiliais(base_url,token)
    
    tasks = [
        (ped,"Pedidos")
    ]
    
    for func,name in tasks:
        try:
            print(f"\n🚀 Iniciando coleta de {name}.")
            logging.info(f"\n🚀 Iniciando coleta de {name}.")
            func.getData(base_url,token,lista_filiais)
        except Exception as e:
            print(f"❌ Erro ao processar {name}: {e}")
            logging.error(f"❌ Erro ao processar {name}: {e}")
            
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    TO_EMAIL = os.getenv("TO_EMAIL")

    if EMAIL_USER and EMAIL_PASSWORD and TO_EMAIL:
        eml.send_log_email(
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