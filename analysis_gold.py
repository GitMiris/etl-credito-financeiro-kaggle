import duckdb
import os

# Garantir que a pasta gold existe
os.makedirs("data_gold", exist_ok=True)

print("Gerando relatório executivo na camada Gold...")

# Definindo a query SQL complexa
query = """
    SELECT 
        CAST(FLOOR(time / 3600) AS INTEGER) % 24 as hora_do_dia,
        count(*) as total_transacoes_fraudulentas,
        round(sum(amount), 2) as valor_total_perdido,
        round(avg(amount), 2) as ticket_medio_fraude
    FROM 'data_silver/credit_card_cleaned.parquet'
    WHERE class = 1
    GROUP BY 1
    ORDER BY valor_total_perdido DESC
"""

# Executando a query e salvando o resultado
# O DuckDB processa o Parquet em milissegundos
df_gold = duckdb.query(query).to_df()

# Salvando o resultado final para o negócio
df_gold.to_parquet('data_gold/relatorio_pico_fraudes.parquet', index=False)

print("--- RELATÓRIO DE IMPACTO FINANCEIRO (TOP 5 HORAS) ---")
print(df_gold.head(5))
print("\nRelatório Gold salvo com sucesso!")


"""
--- RELATÓRIO DE IMPACTO FINANCEIRO (TOP 5 HORAS) ---

   hora_do_dia  total_transacoes_fraudulentas  valor_total_perdido  ticket_medio_fraude
0           11                             51              6570.73               128.84
1           18                             27              6340.14               234.82
2           16                             21              4505.45               214.55
3            2                             48              3753.16                78.19
4           14                             22              3641.12               165.51
"""