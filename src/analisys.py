import sqlite3
import pandas as pd
from pandasql import sqldf

def perform_analysis():
    conn = sqlite3.connect("../tvmaze-analysis/db/tvmaze.db")
    
    query = """
    SELECT 
    id,
    name,
    runtime,
    "_embedded.show.genres" AS genres,
    "_embedded.show.officialSite" AS officialsite,
    "_embedded.show.webChannel.country.name" AS country_name,
    "_embedded.show.webChannel.country.code" AS country_code
    FROM shows s;
    """
    df = pd.read_sql(query, conn)

    # a. Calcular el runtime promedio
    df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce')
    df = df.dropna(subset=['runtime'])
    average_runtime = df['runtime'].mean()
    print(f"Runtime promedio: {average_runtime}")

    # Guardar el DataFrame de runtime promedio
    runtime_df = pd.DataFrame({'average_runtime': [average_runtime]})
    runtime_df.to_parquet("../tvmaze-analysis/db/resultados/average_runtime.snappy.parquet", compression="snappy")

    # b. Conteo de shows por género
    query = """
    SELECT genres, COUNT(1) as conteo
    FROM df
    GROUP BY genres
    """
    result = sqldf(query, locals())
    print(result)

    # Guardar el conteo de géneros
    result.to_parquet("../tvmaze-analysis/db/resultados/genre_count.snappy.parquet", compression="snappy")

    # c. Listar los dominios únicos de los sitios oficiales
    df['domain'] = df['officialsite'].apply(
        lambda x: x.split('/')[2] if isinstance(x, str) and len(x.split('/')) > 2 else None
    )
    unique_domains = df['domain'].dropna().unique()
    print("Dominios únicos de sitios oficiales:")
    print(unique_domains)

    # Guardar los dominios únicos
    domain_df = pd.DataFrame({'unique_domains': unique_domains})
    domain_df.to_parquet("../tvmaze-analysis/db/resultados/unique_domains.snappy.parquet", compression="snappy")

    # Guardar el DataFrame completo para referencia
    df.to_parquet("../tvmaze-analysis/db/resultados/full_data.snappy.parquet", compression="snappy")

if __name__ == "__main__":
    perform_analysis()

