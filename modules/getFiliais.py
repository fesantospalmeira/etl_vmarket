import requests as r
from urllib.parse import urljoin
import pandas as pd
from modules.saveData import save
import logging
def getData(
        baseurl:str,
        token:dict
        )-> list:

    url = urljoin(baseurl,'food/listar')
    try:
        response = r.get(url,headers=token)
        if response.status_code != 200:
            print(f'Erro ao buscar Filiais. {response}')
            logging.error(f'❌ Erro ao buscar Filiais. {response}')
            return lista_filiais == []
        else:
            data = response.json()
            lista_filiais = [item['id_fornecedor'] for item in data]
            lista_filiais.append(89958)
            df = pd.DataFrame(data)
            save(df,'vm_tb_filiais')
            return lista_filiais
    except Exception as e:
        logging.error(f"Erro no código das filiais: \n{e}")
        print(f"Erro no código das filiais: \n{e}")

    

    

