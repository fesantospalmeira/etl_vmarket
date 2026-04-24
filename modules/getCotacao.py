import requests as r
from urllib.parse import urljoin
import pandas as pd
from modules.saveData import save
import logging
def getData(
    baseurl: str,
    token: dict,
    lista_filiais: list
) -> None:
    data_list = []
    for filial in lista_filiais:
        page = 1
        while True:
            url = urljoin(baseurl, 'cotacao/listar')
            params = {
                "id_fornecedor": filial,
                "paginate": 100,
                "page": page
            }

            print(f"🔍 Buscando página {page} para fornecedor {filial}...")
            logging.info(f"🔍 Buscando página {page} para fornecedor {filial}...")
            
            try:
                response = r.get(url, headers=token, params=params)
                if response.status_code != 200:
                    print(f'❌ Erro ao buscar cotações para filial {filial}. Código: {response.status_code} Response:{response.raw}')
                    logging.error(f'❌ Erro ao buscar cotações para filial {filial}. Código: {response.status_code} Response:{response.raw}')
                    break

                data = response.json()
                
                if not data or "data" not in data:
                    print("⚠️ Estrutura inesperada ou sem dados.")
                    break

                cotacoes = data["data"]

                if not cotacoes:
                    print("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    logging.info("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    break

                data_list.extend(cotacoes)

                if data.get("next_page_url") is None:
                    print(f"🏁 Última página atingida para esta filial {filial}.")
                    logging.info(f"🏁 Última página atingida para esta filial {filial}.")
                    break

                page += 1

            except Exception as e:
                print(f"🚨 Erro ao processar filial {filial}: {e}")
                logging.error(f"🚨 Erro ao processar filial {filial}: {e}")
                break

    if not data_list:
        print("⚠️ Nenhuma cotacao encontrado em nenhuma filial.")
        logging.warning("⚠️ Nenhuma cotacao encontrado em nenhuma filial.")
        return

    df = pd.DataFrame(data_list).drop_duplicates()
    save(df, 'vm_tb_cotacoes')
    print(f"💾 {len(df)} Cotações salvas com sucesso!")
    logging.info(f"💾 {len(df)} Cotações salvas com sucesso!")
    print(f"\n🔍 Iniciado detalhamento")
    logging.info(f"\n🔍 Iniciado detalhamento...")

    lista_cotacoes = df['id_cotacao_sisfood'].to_list()
    getDetalhesCotacao(baseurl,token,lista_cotacoes)

def getDetalhesCotacao(
        baseurl:str,
        token:dict,
        lista_cotacao:list
) -> None:
    
    data_list = []
   
    for cotacao in lista_cotacao:
        logging.info(f'Iniciando cotacao {cotacao}')
        url = urljoin(baseurl,f'cotacao/detalhe')
        params = {
            "id_cotacao":cotacao
        }
        try:
            response = r.get(url,headers=token,params=params)
            if response.status_code != 200:
                print(f'Erro ao buscar detalhes de cotação para filial. {response}')
                logging.error(f'Erro ao buscar detalhes de cotação para filial. {response}')
                return None
            else:
                data = response.json()
                if not data or "itens" not in data:
                    print("⚠️ Estrutura inesperada ou sem dados.")
                    logging.warning("⚠️ Estrutura inesperada ou sem dados.")
                    break

                cotacoes = data["itens"]

                if not cotacoes:
                    print("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    logging.info("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    break

                data_list.extend(cotacoes)
                
        except Exception as e:
            print(f"Erro no código dos detalhes de cotação: \n{e}")
            logging.error(f"❌ Erro no código dos detalhes de cotação: \n{e}")
            return None
    
    df = pd.DataFrame(data_list).drop_duplicates()
    save(df,'vm_tb_detalhes_cotacao')
    print(f"💾 {len(df)} detalhes de cotações salvas com sucesso!")
    logging.info(f"💾 {len(df)} detalhes de cotações salvas com sucesso!")