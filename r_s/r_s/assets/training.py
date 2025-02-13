from dagster import asset, Output,AssetIn, MetadataValue
from dagster_dbt import get_asset_key_for_model
from sqlalchemy import create_engine

from r_s.assets.dbt import my_dbt_assets
import pandas as pd


    #config_schema={
    #    "model_trained": bool,
    #    "batch_size": int,
    #    "epochs": int,
    #    "learning_rate": float,
    #    "embeddings_dim": int,
    #},   
    ##ins={
    #"orig_scores": AssetIn(
        # key_prefix=["snowflake", "core"],
        # metadata={"columns": ["id"]}
    #),
    #"orig_movies": AssetIn(
        # key_prefix=["snowflake", "core"],
        # metadata={"columns": ["id"]}
    #),
    #"orig_users": AssetIn(
        # key_prefix=["snowflake", "core"],
        # metadata={"columns": ["id", "user_id", "parent"]}
    #),
   ##}

        #orig_users: pd.DataFrame, 
        #orig_movies: pd.DataFrame, 
        #orig_scores: pd.DataFrame,


@asset(
    group_name="recommender",
    deps=[get_asset_key_for_model([my_dbt_assets], "scores_movies_users")],
    # ins={
    #     "scores_movies_users": AssetIn(
    #         key=get_asset_key_for_model([my_dbt_assets], "scores_movies_users"),
    #         #metadata={"dagster-dbt": "true"}
    #     ),
    # },
    required_resource_keys={"postgres"},
)
def training_data(context) -> Output[pd.DataFrame]:

    #config = context.op_config
    #model_trained = config["model_trained"]
    # model_trained = context.op_config.get("model_trained", False)
    #batch_size = config["batch_size"]
    #epochs = config["epochs"]
    #learning_rate = config["learning_rate"]
    #embeddings_dim = config["embeddings_dim"]   

    ##scores_users = pd.merge(orig_scores, orig_users, left_on='user_id', right_on='id')
    ##all_joined = pd.merge(scores_users, orig_movies, left_on='movie_id', right_on='id')
    
    # Lógica para procesar el asset
    #context.log.info(f"Training with batch_size={batch_size}, epochs={epochs}")

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
        ##all_joined,
        metadata={
            "Total rows": len(scores_movies_users),
            "preview": MetadataValue.md(scores_movies_users.head().to_markdown()),
            #"model_trained": model_trained 
        },
    )

