import pandas as pd
import numpy as np
import os

def run_etl():
    print("Iniciando processo de ETL...")

    # 1. EXTRAÇÃO
    path_bronze = "data_bronze/creditcard.csv"
    if not os.path.exists(path_bronze):
        print("Erro: Arquivo CSV não encontrado na pasta data_bronze!")
        return
    
    # .shape entrega o número de linhas e de colunas.
    df = pd.read_csv(path_bronze)
    print(f"Dados extraídos: {df.shape[0]} linhas")

    # 2. TRANSFORMAÇÃO
    # Padronizando nomes de colunas 
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # Removendo duplicadas
    df = df.drop_duplicates()

    # # Tratamento de Outliers (Apenas transações maiores que zero)
    df = df[df['amount'] > 0]

    # Criando métrica de negócio: Log do valor
    df['log_amount'] = np.log1p(df['amount'])

    # Verificação de Integridade (Remover nulos em colunas críticas)
    cols_criticas = ['time', 'amount', 'class']
    df = df.dropna(subset=cols_criticas)

    # 3. Adicionando coluna de auditoria (Timestamp de processamento)
    # Isso é fundamental para rastreabilidade em grandes bancos
    df['processed_at'] = pd.Timestamp.now()

    # 4. CARGA (Salvando na Silver em PARQUET depois de feito o tratamento)    
    # Removendo o formato CSV para um processamento pesado, porque o Parquet é colunar e muito mais leve.
    os.makedirs("data_silver", exist_ok=True)
    path_silver = "data_silver/credit_card_cleaned.parquet"
    
    # Salvando a versão final tratada
    df.to_parquet(path_silver, index=False)

    print(f"ETL finalizado! Linhas após tratamento: {df.shape[0]}")
    print(f"Dados salvos com sucesso em: {path_silver}")

if __name__ == "__main__":
    run_etl()

# Contém: 284.807 linhas ao todo
# Linhas após tratamento: 281.918