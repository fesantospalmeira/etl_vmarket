from sqlalchemy import create_engine

def create_connection(odbc_params:any) -> any :
    connection_string = f"mssql+pyodbc:///?odbc_connect={odbc_params}"
    print("🔄 Conectando ao banco de dados...")
    try:
        engine = create_engine(connection_string)
        print("✅ Conexão realizada com sucesso!")
        return engine
    
    except Exception as e:
        print(f"❌ Ocorreu um erro ao criar a conexão com o banco de dados: {e}")
        return None