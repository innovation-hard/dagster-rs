from dagster_dbt import DbtCliResource, dbt_assets
from dagster import OpExecutionContext , AssetKey, In, asset, AssetIn

from r_s.constants import dbt_manifest_path

#from r_s.assets.airbyte import airbyte_assets
#from dagster import asset, InputDefinition

@dbt_assets(manifest=dbt_manifest_path)
def my_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    #yield from dbt.cli(["build", "--target", "dev", "--select", "model.db_postgres.scores_movies_users"], context=context).stream()

@asset(
    group_name="dbt_group",
    ins={
        "movies": AssetIn(
            key=AssetKey(["ab_", "movies"]),
        ),         
    }
)
def dbt_movies(context, movies):
    # Esta función solo sirve para establecer la dependencia 
    return None 


@asset(
    group_name="dbt_group",
    ins={
        "users": AssetIn(
            key=AssetKey(["ab_", "users"]),
        ),           
    }
)
def dbt_users(context, users):
    # Esta función solo sirve para establecer la dependencia 
    return None 


@asset(
    group_name="dbt_group",
    ins={
        "scores": AssetIn(
            key=AssetKey(["ab_", "scores"]),
        )                
    }
)
def dbt_scores(context, scores):
    # Esta función solo sirve para establecer la dependencia 
    return None 