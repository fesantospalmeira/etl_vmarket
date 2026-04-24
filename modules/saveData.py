import pandas as pd
from urllib.parse import quote_plus
from modules.createConnectionString import create_connection
import os

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