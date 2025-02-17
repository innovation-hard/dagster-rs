#from dagster_dbt import get_asset_key_for_source, get_asset_key_for_model
#from dagster_mlflow import mlflow_tracking
#from r_s.assets.dbt import my_dbt_assets

from dagster import asset, Output, MetadataValue    #, String, FreshnessPolicy, AssetIn,  Config
from dagster import  AssetKey, asset    #, AssetIn, OpExecutionContext, In
from sqlalchemy import create_engine

import pandas as pd

movies_categories_columns = [
    'unknown', 'Action', 'Adventure', 'Animation',
    "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
    'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
    'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
 
@asset(
    group_name="core",
    #code_version="2",
    deps=[AssetKey(["ab_", "movies"])],
    required_resource_keys={"postgres"},    
    #resource_defs={'postgres': Config('postgres')},
)
def orig_movies(context)-> Output[pd.DataFrame]:
    # Obtener la conexión a PostgreSQL desde el recurso
    #postgres_uri = f"postgresql://{context.resources.postgres['user']}:{context.resources.postgres['password']}@{context.resources.postgres['host']}:{context.resources.postgres['port']}/{context.resources.postgres['database']}"
    postgres_uri='postgresql://airbyte:airbyte@localhost:5432/mlops'
    engine = create_engine(postgres_uri)

    # Leer el dataset "movies" desde PostgreSQL
    query = "SELECT * FROM source.movies;" 
    result = pd.read_sql(query, engine)

    # Renombrar las columnas
    column_mapping = {
        "Children_s": "Children's",
        "Film_Noir": "Film-Noir",
        "Sci_Fi": "Sci-Fi",
    }
    result = result.rename(columns=column_mapping)

    if all(col in result.columns for col in movies_categories_columns):
        sums = result[movies_categories_columns].sum().to_dict()
    else:
        sums = {}    

    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result[movies_categories_columns].sum().to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )



@asset(group_name='core',
    deps=[AssetKey(["ab_", "users"])],
    required_resource_keys={"postgres"},            
)
def orig_users(context)-> Output[pd.DataFrame]:
    postgres_uri='postgresql://airbyte:airbyte@localhost:5432/mlops'
    engine = create_engine(postgres_uri)

    # Leer el dataset "movies" desde PostgreSQL
    query = "SELECT * FROM source.users;"  # Ajusta la consulta según tu esquema
    result = pd.read_sql(query, engine)

    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result.groupby('Occupation').count()['id'].to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )


@asset(
    group_name="core",
    deps=[AssetKey(["ab_", "scores"])],
    required_resource_keys={"postgres","mlflow"}, 
)
def orig_scores(context) -> Output[pd.DataFrame]:
    mlflow = context.resources.mlflow

    postgres_uri='postgresql://airbyte:airbyte@localhost:5432/mlops'
    engine = create_engine(postgres_uri)

    # Leer el dataset "movies" desde PostgreSQL
    query = "SELECT * FROM source.scores;"  # Ajusta la consulta según tu esquema
    result = pd.read_sql(query, engine)

    metrics = {
        "Total rows": len(result),
        "scores_mean": float(result['rating'].mean()),
        "scores_std": float(result['rating'].std()),
        "unique_movies": len(result['movie_id'].unique()),
        "unique_users": len(result['user_id'].unique())
    }
    mlflow.log_metrics(metrics)

    return Output(
        result,
        metadata=metrics,
    )
