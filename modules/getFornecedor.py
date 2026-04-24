import requests as r
from urllib.parse import urljoin
import pandas as pd
from modules.saveData import save
import logging

def getData(
        baseurl:str,
        token:dict,
        lista_filiais:list
)-> None:
    data_list = []
    for filial in lista_filiais:
        url = urljoin(baseurl,f'fornecedor/listar')
        params = {
            "id_fornecedor":filial
        }
        print(f"\n🚀 Iniciando filial {filial}")
        try:
            response = r.get(url,headers=token,params=params)
            if response.status_code != 200:
                print(f'❌ Erro ao buscar fornecedores para filial {filial}. {response}')
                logging.error(f'❌ Erro ao buscar fornecedores para filial {filial}. {response}')
                return None
            else:
                data = response.json()
                data_list.extend(data)
                
        except Exception as e:
            print(f"❌ Erro no código das filiais: \n{e}")
            logging.error(f"❌ Erro no código das filiais: \n{e}")
            return None
    
    df = pd.DataFrame(data_list).drop_duplicates()
    save(df,'vm_tb_fornecedores')
    print(f"💾 {len(df)} fornecedores salvos com sucesso!")
    logging.info(f"💾 {len(df)} fornecedores salvos com sucesso!")



    