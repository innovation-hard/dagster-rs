from dagster import asset, Output, MetadataValue, String, FreshnessPolicy, AssetIn,  Config
from dagster_mlflow import mlflow_tracking
#from dagster_dbt import get_asset_key_for_model,DbtCliResource, dbt_assets
#from r_s.assets.airbyte import airbyte_assets
from r_s.assets.dbt import my_dbt_assets
import pandas as pd

movies_categories_columns = [
    'unknown', 'Action', 'Adventure', 'Animation',
    "Children's", 'Comedy', 'Crime', 'Documentary', 'Drama',
    'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
    'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
 
@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    group_name="core",
    code_version="2",
    config_schema={'uri': String}
)

def orig_movies(context) -> Output[pd.DataFrame]:
    #uri = context.op_config['uri']
    uri='https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'
    context.log.info(uri)
    result = pd.read_csv(uri)
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result[movies_categories_columns].sum().to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )

    # group_name='csv_data',
    # io_manager_key="parquet_io_manager",
    # partitions_def=hourly_partitions,
    # key_prefix=["s3", "core"],
@asset(group_name='core',
       #config_schema={'uri': String}       
    )
def orig_users() -> Output[pd.DataFrame]:
    uri = 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/usuarios_0.csv'
    result = pd.read_csv(uri)
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result.groupby('Occupation').count()['id'].to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )

    # io_manager_key="parquet_io_manager",
    # partitions_def=hourly_partitions,
    # key_prefix=["s3", "core"],
@asset(
    group_name="core",
    #config_schema={'uri': String},
    resource_defs={'mlflow': mlflow_tracking}
)
def orig_scores(context) -> Output[pd.DataFrame]:
    mlflow = context.resources.mlflow
    #mlflow = {'config': {'experiment_name': 'r_s'}}
    uri = 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/scores_0.csv'
    result = pd.read_csv(uri)
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
