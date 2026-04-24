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
            print("✅ Token gerado com sucesso!")
            logging.info("✅ Token gerado com sucesso!")
            
            return token_final
        except r.exceptions.JSONDecodeError:
            print(f"❌ Resposta (Texto): {response.text}")
            logging.error(f"❌ Resposta (Texto): {response.text}")
            return None

    except r.exceptions.ConnectionError as errc:
        print(f"❌ Erro de Conexão: {errc}")
        logging.error(f"❌ Erro de Conexão: {errc}")
        return None
    except r.exceptions.RequestException as err:
        print(f"❌ Erro na Requisição: {err}")
        logging.error(f"❌ Erro na Requisição: {err}")
        return None