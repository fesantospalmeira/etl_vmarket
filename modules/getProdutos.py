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
            url = urljoin(baseurl, 'produto/listar')
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
                    print(f'❌ Erro ao buscar produtos para filial {filial}. Código: {response.status_code} Response:{response.raw}')
                    logging.error(f'❌ Erro ao buscar produtos para filial {filial}. Código: {response.status_code} Response:{response.raw}')
                    break

                data = response.json()

                if not data or "data" not in data:
                    print("⚠️ Estrutura inesperada ou sem dados.")
                    logging.warning("⚠️ Estrutura inesperada ou sem dados.")
                    break

                produtos = data["data"]
                if not produtos:
                    print("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    logging.info("✅ Nenhum item encontrado nesta página, finalizando coleta da filial.")
                    break

                data_list.extend(produtos)

                if data.get("next_page_url") is None:
                    print("🏁 Última página atingida para esta filial.")
                    logging.info("🏁 Última página atingida para esta filial.")
                    break

                page += 1

            except Exception as e:
                print(f"❌ Erro ao processar filial {filial}: {e}")
                logging.error(f"❌ Erro ao processar filial {filial}: {e}")
                break

    if not data_list:
        print("⚠️ Nenhum produto encontrado em nenhuma filial.")
        logging.warning("⚠️ Nenhum produto encontrado em nenhuma filial.")
        return

    df = pd.DataFrame(data_list).drop_duplicates()
    save(df, 'vm_tb_produtos')

    print(f"💾 {len(df)} produtos salvos com sucesso!")
    logging.info(f"💾 {len(df)} produtos salvos com sucesso!")
