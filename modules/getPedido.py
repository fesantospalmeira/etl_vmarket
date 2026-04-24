from datetime import datetime, timedelta
import time
import pandas as pd
from urllib.parse import urljoin
import requests as r
import logging
from modules.saveData import save
from modules.getToken import getToken
import json
from dotenv import load_dotenv
import os

load_dotenv()

def getData(
    baseurl: str,
    token: dict,
    lista_filiais: list
) -> None:
    email = os.getenv('EMAIL')
    password = os.getenv('PASSWORD')

    data_list = []
    for filial in lista_filiais:
        page = 1
        
        while True:
        # while page == 1:
                url = urljoin(baseurl, 'pedido/listar')
                params = {
                    "paginate": 100,
                    "page": page,
                    "id_fornecedor": filial
                }

                print(f"🔍 Buscando página {page} para filial {filial}...")
                logging.info(f"🔍 Buscando página {page} para filial {filial}...")
                try:
                    response = r.get(url, headers=token, params=params)
                    if response.status_code != 200:
                        msg = f'❌ Erro ao buscar pedidos. Código: {response.status_code} Response:{response.text[:100]}'
                        print(msg)
                        logging.error(msg)
                        break

                    data = response.json()
                    
                    if not data or "data" not in data:
                        print("⚠️ Estrutura inesperada ou sem dados.")
                        logging.warning("⚠️ Estrutura inesperada ou sem dados.")
                        break

                    pedidos = data["data"]

                    if not pedidos:
                        print("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                        logging.info("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                        break

                    data_list.extend(pedidos)

                    if data.get("next_page_url") is None:
                        print("🏁 Última página atingida para esta filial.")
                        logging.info("🏁 Última página atingida para esta filial.")
                        break

                    page += 1
                    time.sleep(3)
                except Exception as e:
                    print(f"❌ Erro ao processar: {e}")
                    logging.error(f"❌ Erro ao processar: {e}")
                    continue

    if not data_list:
        print("⚠️ Nenhuma pedido encontrado em nenhuma filial.")
        logging.warning("⚠️ Nenhuma pedido encontrado em nenhuma filial.")
        return


    df = pd.DataFrame(data_list).drop_duplicates()
    save(df, 'vm_tb_pedidos')
    print(f"💾 {len(df)} pedidos salvos com sucesso!")
    logging.info(f"💾 {len(df)} pedidos salvos com sucesso!")
    print(f"\n🔍 Iniciado detalhamento de pedidos..")
    logging.info(f"\n🔍 Iniciado detalhamento de pedidos..")

    # df['dt_inclusao'] = pd.to_datetime(df['dt_inclusao'])
    lista_pedidos = df['id_pedido'].to_list()
    print(len(lista_pedidos))
    token_1 = getToken(baseurl,email,password)
    getDetalhesPedido(baseurl,token_1,lista_pedidos,email,password) 
    time.sleep(10)
    token_2 = getToken(baseurl,email,password)
    lista_pedidos_divergentes = getPedidosDivergentes(baseurl,token_2,lista_filiais)
    time.sleep(10)
    token_3 = getToken(baseurl,email,password)
    getRelatorioPedido(baseurl,token_3,lista_pedidos_divergentes)
    
def getDetalhesPedido(
        baseurl:str,
        token:dict,
        lista_pedidos:list,
        email:str,
        password:str
) -> None:
    
    data_list = []
    
    for i, id_pedido in enumerate(lista_pedidos):
        if i > 0 and i % 50 == 0:
            logging.info(f'Chegando no pedido número {i}, alterando token....')
            token = getToken(baseurl, email, password)

        time.sleep(5)
        logging.info(f'Iniciando pedido {id_pedido}')
        page = 1
        
        while True:
            url = urljoin(baseurl, f'pedido/detalhe')
            params = {
                "id_pedido": id_pedido,
                "page": page
            }
            
            sucesso_requisicao = False
            response = None
            
            for tentativa in range(5):
                try:
                    response = r.get(url, headers=token, params=params)
                    
                    if response.status_code == 200:
                        sucesso_requisicao = True
                        break 
                    else:
                        mensagem_erro = f'Erro {response.status_code} na requisição. Tentativa {tentativa + 1}/5. Renovando token...'
                        print(mensagem_erro)
                        logging.info(mensagem_erro)
                        token = getToken(baseurl, email, password)
                        time.sleep(2) 
                        
                except Exception as e:
                    mensagem_excecao = f"⚠️ Exceção na requisição: {e}. Tentativa {tentativa + 1}/5. Renovando token..."
                    print(mensagem_excecao)
                    logging.info(mensagem_excecao)
                    
                    token = getToken(baseurl, email, password)
                    time.sleep(2)
            
            if not sucesso_requisicao:
                aviso_final = f'❌ Falha definitiva ao buscar a página {page} do pedido {id_pedido} após 5 tentativas. Pulando para o próximo pedido.'
                print(aviso_final)
                logging.error(aviso_final)
                break 
            
            data = response.json()
            if not data or "itens" not in data:
                print("⚠️ Estrutura inesperada ou sem dados.")
                logging.warning("⚠️ Estrutura inesperada ou sem dados.")
                break

            pedido = data["itens"]["data"]

            if not pedido:
                print(f"✅ Nenhum item encontrado na página {page}, finalizando coleta do pedido {id_pedido}.")
                logging.info(f"✅ Nenhum item encontrado na página {page}, finalizando coleta do pedido {id_pedido}.")
                break

            data_list.extend(pedido)
            
            if data.get("next_page_url") is None:
                break
            
            page += 1
            
    df = pd.DataFrame(data_list)
    save(df, 'vm_tb_detalhes_pedido')
    print(f"💾 {len(df)} detalhes de pedidos salvos com sucesso!")
    logging.info(f"💾 {len(df)} detalhes de pedidos salvos com sucesso!")
    

def getPedidosDivergentes(
        baseurl: str,
        token: dict,
        lista_filiais: list
) -> list:
    
    data_list = []
    current_date = datetime(2025, 12, 29)
    end_date = datetime.now()
    url_exata = "https://integracao-compras.vmarketcompras.com.br/api/nota-fiscal/buscar-pedidos-divergencia-depara"

    print("\n🔍 Iniciando busca de pedidos divergentes...")
    logging.info("\n🔍 Iniciando busca de pedidos divergentes...")

    while current_date <= end_date:
        chunk_end_date = current_date + timedelta(days=2)
        chunk_end_date = chunk_end_date.replace(hour=23, minute=59, second=59)
        if chunk_end_date > end_date:
            chunk_end_date = end_date
        str_data_de = current_date.strftime("%Y-%m-%d %H:%M:%S")
        str_data_ate = chunk_end_date.strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "data_de": str_data_de,
            "data_ate": str_data_ate,
            "filias": lista_filiais 
        }
        headers = token.copy() 
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json" 
        headers["User-Agent"] = "PostmanRuntime/7.36.1"
        
        print(f"📅 Consultando período: {str_data_de} a {str_data_ate}...")
        logging.info(f"📅 Consultando período: {str_data_de} a {str_data_ate}...")
        
        try:
            response = r.post(url_exata, headers=headers, json=payload, allow_redirects=False)
            
            if response.status_code != 200:
                print(f'❌ Erro: {response.status_code}')
                print(f'❌ Resposta: {response.text}')
                logging.warning(f'❌ Resposta: {response.text}')
                if response.status_code in [301, 302, 307, 308]:
                    print(f"⚠️ A API tentou redirecionar para: {response.headers.get('Location')}")
                    logging.warning(f"⚠️ A API tentou redirecionar para: {response.headers.get('Location')}")
            else:
                data = response.json()
                total_divergencias = data.get("total_pedidos_com_divergencia", 0)
                
                if total_divergencias == 0:
                    print("✅ Zero divergências neste período. Pulando...")
                    logging.info("✅ Zero divergências neste período. Pulando...")
                else:
                    itens_pedido_nf = data.get("itensPedidoNF", {})

                    for id_pedido, info in itens_pedido_nf.items():
                        divergentes = info.get("divergentes", [])
                        
                        for item in divergentes:
                            row = item.copy()
                            row["id_pedido"] = id_pedido
                            row["data_de_busca"] = str_data_de  
                            row["data_ate_busca"] = str_data_ate 
                            data_list.append(row)
        except Exception as e:
            print(f"❌ Erro na requisição: {e}")
            logging.error(f"❌ Erro na requisição: {e}")
        current_date = current_date + timedelta(days=3)
        current_date = current_date.replace(hour=0, minute=0, second=0)
        if current_date <= end_date:
            print("⏳ Aguardando 30 segundos para a próxima requisição...")
            # time.sleep(30)
            
    if not data_list:
        print("⚠️ Nenhuma divergência encontrada em todo o período analisado.")
        logging.warning("⚠️ Nenhuma divergência encontrada em todo o período analisado.")
        return

    df = pd.DataFrame(data_list).drop_duplicates(subset=['id_pedido', 'nome']) 
    for coluna in df.columns:
        df[coluna] = df[coluna].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
    save(df, 'vm_tb_divergencias')

    print(f"💾 {len(df)} itens com divergência salvos com sucesso no banco!")
    logging.info(f"💾 {len(df)} itens com divergência salvos com sucesso no banco!")
    
    return df['id_pedido'].to_list()
    
def getRelatorioPedido(
        baseurl: str,
        token: dict,
        lista_pedidos: list
) -> None:
    
    data_list = []
    
    print("\n🔍 Iniciando busca de relatórios de pedidos (JSON)...")
    logging.info("\n🔍 Iniciando busca de relatórios de pedidos (JSON)...")

    for id_pedido in lista_pedidos:
        url = urljoin(baseurl,f'pedido/relatorio-json')
        params = {
            "id_pedido": id_pedido
        }

        try:
            response = r.get(url, headers=token, params=params)
            
            # --- TRATAMENTO DOS CÓDIGOS DE ERRO DA API ---
            if response.status_code != 200:
                try:
                    error_data = response.json()
                    cod_info = int(error_data.get("cod_info", -1))
                    msg = error_data.get("message", "Erro desconhecido")
                except:
                    cod_info = -1
                    msg = response.text

                # 404 Not Found (cod_info 3 ou 4): Pedido não existe ou não tem relatório
                if response.status_code == 404 and cod_info in [3, 4]:
                    print(f"ℹ️ Pedido {id_pedido} ignorado: {msg}")
                    logging.info(f"ℹ️ Pedido {id_pedido} ignorado: {msg}")
                    continue 

                # 401 Unauthorized (cod_info 1 ou 2): Erro de Token (FATAL)
                elif response.status_code == 401 and cod_info in [1, 2]:
                    print(f"🛑 ERRO CRÍTICO (401): {msg}. Interrompendo toda a extração.")
                    logging.error(f"🛑 ERRO CRÍTICO (401): {msg}. Interrompendo toda a extração.")
                    return 

                # 400 Bad Request (cod_info 2): ID vazio/obrigatório
                elif response.status_code == 400 and cod_info == 2:
                    print(f"⚠️ ID do pedido incorreto ou vazio (400): {msg}")
                    logging.warning(f"⚠️ ID do pedido incorreto ou vazio (400): {msg}")
                    continue

                # 500 Internal Server Error (cod_info 5): Erro interno na VMarket
                elif response.status_code == 500 and cod_info == 5:
                    print(f"❌ Erro na API (500) ao buscar pedido {id_pedido}: {msg}")
                    logging.error(f"❌ Erro na API (500) ao buscar pedido {id_pedido}: {msg}")
                    continue

                # Qualquer outro erro não documentado
                else:
                    print(f"❌ Erro não mapeado ({response.status_code}) no pedido {id_pedido}: {msg}")
                    logging.error(f"❌ Erro não mapeado ({response.status_code}) no pedido {id_pedido}: {msg}")
                    continue

            data = response.json()
            
            if not data or "relatorio_json" not in data:
                print(f"⚠️ Estrutura inesperada para o pedido {id_pedido}.")
                logging.warning(f"⚠️ Estrutura inesperada para o pedido {id_pedido}.")
                continue

            url_relatorio = data.get("url_relatorio", "")
            relatorio = data.get("relatorio_json", {})
            
            dados_nf = relatorio.get("dados_nota_fiscal", {})
            produtos = relatorio.get("produtos", [])
            totais = relatorio.get("totais", {})
            
            if not isinstance(dados_nf, dict):
                dados_nf = {}
            if not isinstance(totais, dict):
                totais = {}
            if not isinstance(produtos, list):
                produtos = []

            if produtos:
                for produto in produtos:
                    # Protege o produto individual também
                    if not isinstance(produto, dict):
                        produto = {}
                        
                    row = {
                        "id_pedido_busca": id_pedido,
                        "url_relatorio": url_relatorio,
                        **dados_nf,
                        **produto,
                        **totais
                    }
                    data_list.append(row)
            else:
                row = {
                    "id_pedido_busca": id_pedido,
                    "url_relatorio": url_relatorio,
                    **dados_nf,
                    **totais
                }
                data_list.append(row)

        except Exception as e:
            print(f"❌ Erro na execução local para o pedido {id_pedido}: \n{e}")
            logging.error(f"❌ Erro na execução local para o pedido {id_pedido}: \n{e}")
            continue
        # time.sleep(30)
    if not data_list:
        print("⚠️ Nenhum relatório foi extraído com sucesso.")
        logging.warning("⚠️ Nenhum relatório foi extraído com sucesso.")
        return

    df = pd.DataFrame(data_list).drop_duplicates()
    save(df, 'vm_tb_relatorio_pedidos')
    
    print(f"💾 {len(df)} itens de relatórios salvos com sucesso!")
    logging.info(f"💾 {len(df)} itens de relatórios salvos com sucesso!")