import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno
from jinja2 import Template
import os

def profile_data():
    # Cargar el dataset
    df = pd.read_parquet("../tvmaze-analysis/data/tvmaze_raw.parquet")
    
    # Verificar los tipos de datos en el DataFrame
    print("Tipos de datos en el DataFrame:")
    print(df.dtypes)
    
    # Intentar convertir todas las columnas a string y eliminar duplicados
    df_cleaned = df.astype(str).drop_duplicates()
    print(f"Filas duplicadas eliminadas: {df.shape[0] - df_cleaned.shape[0]} filas")
    
    # Ejemplo de limpieza: eliminando columnas con muchos valores nulos
    df_cleaned = df_cleaned.dropna(axis=1, thresh=int(0.5*len(df_cleaned)))  # Eliminar columnas con más del 50% de valores nulos

    # Forzar la conversión de columnas numéricas
    numeric_columns = df_cleaned.select_dtypes(include='number')
    df_cleaned[numeric_columns.columns] = df_cleaned[numeric_columns.columns].apply(pd.to_numeric, errors='coerce')

    # Guardar el DataFrame en un archivo Snappy Parquet
    cleaned_parquet_path = "../tvmaze-analysis/data/tvmaze_cleaned.snappy.parquet"
    df_cleaned.to_parquet(cleaned_parquet_path, compression='snappy')
    print(f"DataFrame limpio guardado en: {cleaned_parquet_path}")
    
    # Generar un resumen descriptivo del dataframe
    stats = df_cleaned.describe().to_html()  # Estadísticas descriptivas como HTML
    
    # Convertir los valores faltantes a un DataFrame para luego usar `to_html()`
    missing = pd.DataFrame(df_cleaned.isnull().sum(), columns=['Missing Values']).to_html()  # Missing values en HTML
    
    # Crear un directorio para las imágenes si no existe
    img_dir = "../tvmaze-analysis/profiling/images"
    os.makedirs(img_dir, exist_ok=True)
    
    # Visualización de los valores faltantes con missingno
    msno_matrix = msno.matrix(df_cleaned)
    msno_matrix.figure.savefig(os.path.join(img_dir, "missing_data.png"))  # Guardamos la imagen de missingno
    
    # Generar histogramas solo para las columnas numéricas
    numeric_columns = df_cleaned.select_dtypes(include='number')
    if not numeric_columns.empty:
        numeric_columns.hist(bins=50, figsize=(20, 15))
        plt.savefig(os.path.join(img_dir, "histograms.png"))  # Guardamos la imagen de los histogramas
    else:
        print("No hay columnas numéricas para generar histogramas.")
    
    # Filtrar solo las columnas numéricas para la correlación
    correlation_matrix = df_cleaned[numeric_columns.columns].corr() if not numeric_columns.empty else pd.DataFrame()
    
    # Correlación Heatmap
    if not correlation_matrix.empty:
        plt.figure(figsize=(12, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
        plt.savefig(os.path.join(img_dir, "correlation_heatmap.png"))  # Guardamos la imagen del heatmap
    else:
        print("No se puede generar un heatmap de correlación porque no hay columnas numéricas.")
    
    # Crear HTML usando Jinja2
    html_template = """
    <html>
    <head>
        <title>TVMaze Data Profiling Report</title>
    </head>
    <body>
        <h1>TVMaze Data Profiling Report</h1>
        <h2>Descriptive Statistics</h2>
        {{ stats|safe }}
        
        <h2>Missing Values</h2>
        {{ missing|safe }}
        
        <h2>Histograms of Numeric Columns</h2>
        <img src="images/histograms.png" width="800">
        
        <h2>Correlation Heatmap</h2>
        <img src="images/correlation_heatmap.png" width="800">
    </body>
    </html>
    """
    
    # Cargar el template y renderizar el HTML
    template = Template(html_template)
    html_content = template.render(stats=stats, missing=missing)
    
    # Guardar el HTML
    with open("../tvmaze-analysis/profiling/tvmaze_profile.html", "w") as f:
        f.write(html_content)
    
    print("Profiling report saved as HTML.")

if __name__ == "__main__":
    profile_data()
