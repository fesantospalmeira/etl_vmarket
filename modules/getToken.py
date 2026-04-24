import requests as r
from urllib.parse import urljoin
import logging

def getToken(
        base_url:str,
        email:str,
        password:str
        ) -> dict:
    
    
    login = {
        "email": email,
        "password":password
    }
    auth_url = urljoin(base_url, 'autenticar')
    try:
        response = r.post(auth_url, json=login)
        response.raise_for_status() 
        try:
            token = response.json()
            token_final = {
                "Authorization": f'Bearer {token['token']}'
            }
            msg = "✅ Token gerado com sucesso!"
            
            print(msg)
            logging.info(msg)
            
            return token_final
        except r.exceptions.JSONDecodeError:
            msg = f"❌ Erro ao decodificar JSON de Token: {response.text}"
            print(msg)
            logging.error(msg)
            return None

    except r.exceptions.ConnectionError as errc:
        msg = f"❌ Erro de Conexão com a API de Geração de Token: {errc}"
        print(msg)
        logging.error(msg)
        return None
    except r.exceptions.RequestException as err:
        msg = f"❌ Erro na Requisição de Geração de Token: {err}"
        print(msg)
        logging.error(msg)
        return None