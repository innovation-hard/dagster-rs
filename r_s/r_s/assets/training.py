from dagster import asset, Output,AssetIn, MetadataValue
from dagster_dbt import get_asset_key_for_model
from sqlalchemy import create_engine

from r_s.assets.dbt import my_dbt_assets
import pandas as pd

@asset(
    group_name="recommender",
    deps=[get_asset_key_for_model([my_dbt_assets], "scores_movies_users")],
    required_resource_keys={"postgres"},
)
def training_data(context) -> Output[pd.DataFrame]:
    # Obtener la conexión a PostgreSQL desde el recurso
    #postgres_uri = f"postgresql://{context.resources.postgres['username']}:{context.resources.postgres['password']}@{context.resources.postgres['hostname']}:{context.resources.postgres['port']}/{context.resources.postgres['db_name']}"
    #postgres_uri = f"postgresql://{context.resources.postgres.username}:{context.resources.postgres.password}@{context.resources.postgres.hostname}:{context.resources.postgres.port}/{context.resources.postgres.db_name}"
    postgres_uri='postgresql://airbyte:airbyte@localhost:5432/mlops'

    with open("/home/repo/ML/dagster-rs/engine.txt", "w") as f:
        f.write(postgres_uri)

    engine = create_engine(postgres_uri)

    # Leer el dataset "scores_movies_users" desde PostgreSQL
    query = "SELECT * FROM target.scores_movies_users;"
    scores_movies_users = pd.read_sql(query, engine)

    return Output(
        scores_movies_users,
        metadata={
            "Total rows": len(scores_movies_users),
            "preview": MetadataValue.md(scores_movies_users.head().to_markdown()),
            #"model_trained": model_trained 
        },
    )

