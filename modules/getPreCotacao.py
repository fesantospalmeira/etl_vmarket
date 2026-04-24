import requests as r
from urllib.parse import urljoin
import pandas as pd
from modules.saveData import save
import logging

def getData(
    baseurl: str,
    token: dict,
    lista_filiais:dict
) -> None:
    data_list = []
    page = 1
    
    while True:
            url = urljoin(baseurl, 'precotacao/listar')
            params = {
                "ativo": 0,
                "expirado":0,
                "encerrado":0,
                "paginate": 100,
                "page": page
            }

            print(f"🔍 Buscando página {page} ....")
            logging.info(f"🔍 Buscando página {page} ....")

            try:
                response = r.get(url, headers=token, params=params)
                if response.status_code != 200:
                    print(f'❌ Erro ao buscar precotações. Código: {response.status_code} Response:{response.raw}')
                    logging.error(f'❌ Erro ao buscar precotações. Código: {response.status_code} Response:{response.raw}')
                    break
                
                data = response.json()
                
                if not data or "data" not in data:
                    print("⚠️ Estrutura inesperada ou sem dados.")
                    logging.warning("⚠️ Estrutura inesperada ou sem dados.")
                    break

                precotacoes = data["data"]

                if not precotacoes:
                    print("✅ Nenhum item encontrado nesta página, finalizando coleta.")
                    logging.info("✅ Nenhum item encontrado nesta página, finalizando coleta.")
                    break

                data_list.extend(precotacoes)

                if data.get("next_page_url") is None:
                    print("🏁 Última página atingida .")
                    logging.info("🏁 Última página atingida .")
                    break
                page += 1

            except Exception as e:
                print(f"❌ Erro ao processar: {e}")
                logging.info(f"❌ Erro ao processar: {e}")
                break

    df = pd.DataFrame(data_list).drop_duplicates()
    save(df, 'vm_tb_pre_cotacao')
    print(f"💾 {len(df)} pre cotações salvas com sucesso!")
    logging.info(f"💾 {len(df)} pre cotações salvas com sucesso!")

    print(f"\n🔍 Iniciado detalhamento")
    logging.info(f"\n🔍 Iniciado detalhamento...")
    
    lista_pre_cotacoes = df['id_precotacao'].to_list()
    getDetalhesPreCotacao(baseurl,token,lista_pre_cotacoes)
    

def getDetalhesPreCotacao(
    baseurl: str,
    token: dict,
    lista_cotacao: list
) -> None:

    data_list = []

    for cotacao in lista_cotacao:
        logging.info(f'Iniciando precotacao:{cotacao}')
        url = urljoin(baseurl, 'precotacao/detalhe')
        params = {"id_precotacao": cotacao}

        try:
            response = r.get(url, headers=token, params=params)
            if response.status_code != 200:
                print(f'Erro ao buscar detalhes da cotação {cotacao}.')
                logging.error(f'Erro ID {cotacao}: {response.status_code}')
                continue

            data = response.json()

            if not data:
                logging.warning(f"⚠️ Sem dados para cotação {cotacao}.")
                continue
            data_list.append(data)

        except Exception as e:
            print(f"Erro na requisição ID {cotacao}: {e}")
            logging.error(f"❌ Erro ID {cotacao}: {e}")
            continue
    if not data_list:
        print("Nenhum detalhe coletado.")
        return

    print("🔄 Processando e estruturando os dados...")

    df_filiais = pd.json_normalize(
        data_list,
        record_path=['filiais_participantes'],
        meta=['id_precotacao', 'descricao', 'data_vencimento'], 
        errors='ignore'
    )

    if df_filiais.empty:
        print("⚠️ Dados coletados, mas nenhuma filial encontrada.")
        return
    lista_itens_final = []
    for index, row in df_filiais.iterrows():
        itens = row.get('itens', [])
        
        # Se houver itens, normalizamos eles
        if isinstance(itens, list) and len(itens) > 0:
            df_temp = pd.DataFrame(itens)
            df_temp['id_precotacao'] = row['id_precotacao']
            df_temp['id_precotacao_filial'] = row['id_precotacao_filial'] 
            df_temp['nome_fantasia_filial'] = row['nome_fantasia']
            df_temp['data_vencimento'] = row['data_vencimento']
            
            lista_itens_final.append(df_temp)
        else:
            linha_vazia = {
                'id_precotacao': row['id_precotacao'],
                'id_precotacao_filial': row['id_precotacao_filial'],
                'nome_fantasia_filial': row['nome_fantasia'],
                'data_vencimento': row['data_vencimento'],
                'id_produto_sisfood_cotacao': None,
                'nome': None, 
                'quantidade': 0, 
                'unidade': None,
                'marca': None
            }
            df_temp = pd.DataFrame([linha_vazia])
            lista_itens_final.append(df_temp)

    if not lista_itens_final:
        print("⚠️ Filiais encontradas, mas nenhum item/produto listado nelas.")
        return

    df_final = pd.concat(lista_itens_final, ignore_index=True)

    colunas_desejadas = [
        'id_precotacao', 
        'id_precotacao_filial', 
        'nome_fantasia_filial',
        'id_produto_sisfood_cotacao', 
        'nome', 
        'quantidade', 
        'unidade', 
        'marca'
    ]
    
    cols_existentes = [c for c in colunas_desejadas if c in df_final.columns]
    df_final = df_final[cols_existentes]

    save(df_final, 'vm_tb_detalhes_pre_cotacao')
    print(f"💾 {len(df_final)} itens de precotações salvos com sucesso!")
    logging.info(f"💾 {len(df_final)} itens salvos.")