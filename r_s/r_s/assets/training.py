from dagster import asset, Output #, MetadataValue, String, AssetIn, FreshnessPolicy, Config
#from dagster_mlflow import mlflow_tracking
from dagster_dbt import get_asset_key_for_model #,DbtCliResource, dbt_assets
#from r_s.assets.airbyte import airbyte_assets
from r_s.assets.dbt import my_dbt_assets
import pandas as pd


@asset(group_name="recommender",
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
   deps=[get_asset_key_for_model([my_dbt_assets], "scores_movies_users")],
)
def training_data(
        scores_movies_users: pd.DataFrame, 
        #orig_users: pd.DataFrame, 
        #orig_movies: pd.DataFrame, 
        #orig_scores: pd.DataFrame,
    ) -> Output[pd.DataFrame]:
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
    return Output(
        scores_movies_users,
        ##all_joined,
        metadata={
            "Total rows": len(scores_movies_users),
            #"model_trained": model_trained  # Si aplica
        },
    )