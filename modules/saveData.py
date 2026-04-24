import pandas as pd
from urllib.parse import quote_plus
from modules.createConnectionString import create_connection
from sqlalchemy import text, bindparam
import os
import logging

def save(df: pd.DataFrame, table: str) -> None:
    if not df.empty:
        DB_SERVER = os.getenv("DB_SERVER")
        DB_DATABASE = os.getenv("DB_DATABASE")
        DRIVER = os.getenv("DRIVER")
        
        odbc_params = quote_plus(
                f"DRIVER={{{DRIVER}}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_DATABASE};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"  
            )
        engine = create_connection(odbc_params)
        
        print(f"📤 Inserindo dados na tabela '{table}'...")
        df.to_sql(name=table, con=engine, if_exists='replace', index=False, chunksize=1000)
        print(f"Dados inseridos com sucesso na tabela {table}!")
    else:
        print('⚠️ Sem dados para inserir..')
        
def save_with_period(df: pd.DataFrame, table:str, p1:str, p2:str,field_date:str) -> None:
    if not df.empty:
        DB_SERVER = os.getenv("DB_SERVER")
        DB_DATABASE = os.getenv("DB_DATABASE")
        DRIVER = os.getenv("DRIVER")
        
        odbc_params = quote_plus( 
                f"DRIVER={{{DRIVER}}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_DATABASE};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"  
            )
        engine = create_connection(odbc_params)
        query = text(f"DELETE FROM {table} WHERE {field_date} BETWEEN :start AND :end")
        try:
            with engine.begin() as connection:  
                msg = '📤 Deletando dados antes de inserir..'
                print(msg)
                logging.info(msg)
                connection.execute(query, {"start": p1, "end": p2})
                print(f"📤 Inserindo dados na tabela '{table}'...")
                df.to_sql(name=table, con=connection, if_exists='append', index=False, chunksize=1000)
                
            print(f"Dados inseridos com sucesso na tabela {table}!")
        except Exception as e:
            print(f"❌ Erro crítico ao processar {table}. As alterações foram desfeitas.")
            logging.error(f"Erro no banco de dados para {table}: {e}")
    else:
        print('⚠️ Sem dados para inserir..')

def save_with_id(df:pd.DataFrame,table:str,id_field_name:str) -> None:
    if not df.empty:
        DB_SERVER = os.getenv("DB_SERVER")
        DB_DATABASE = os.getenv("DB_DATABASE")
        DRIVER = os.getenv("DRIVER")
        
        odbc_params = quote_plus(
                f"DRIVER={{{DRIVER}}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_DATABASE};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"  
            )
        engine = create_connection(odbc_params)
        id_tuple = tuple(df[id_field_name].to_list())
        
        try:
            with engine.begin() as connection:  
                msg = '📤 Deletando dados antigos por ID...'
                print(msg)
                logging.info(msg)
                
                chunk_size = 2000
                total_apagados = 0
                for i in range(0, len(id_tuple), chunk_size):
                    id_chunk  = id_tuple[i:i + chunk_size]
                    
                    query = text(f"DELETE FROM {table} WHERE {id_field_name} in :ids")
                    query = query.bindparams(bindparam('ids', expanding=True))
                    
                    result = connection.execute(query, {"ids": id_chunk})
                    total_apagados += result.rowcount
                    
                msg_apagados = f"🗑️ Total de registros antigos apagados: {total_apagados}"
                print(msg_apagados)
                logging.info(msg_apagados)
                
                print(f"📤 Inserindo novos dados na tabela '{table}'...")
                df.to_sql(name=table, con=connection, if_exists='append', index=False, chunksize=1000)
            total_inserido = len(df)
            msg =  f"✅ {total_inserido} registros inseridos com sucesso na tabela {table}!"
            print(msg)
            logging.info(msg)
            
        except Exception as e:
            print(f"❌ Erro crítico ao processar {table}. As alterações foram desfeitas.")
            logging.error(f"Erro no banco de dados para {table}: {e}")
    else:
        print('⚠️ Sem dados para inserir..')
    
    