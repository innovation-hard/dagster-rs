from dagster import Definitions #, ScheduleDefinition
from dagster import define_asset_job,job #, AssetSelection, 

from dagster_dbt import DbtCliResource
from r_s.assets.dbt import dbt_users,dbt_movies,dbt_scores,my_dbt_assets
from r_s.constants import dbt_project_dir

from dagster_airbyte import airbyte_resource,load_assets_from_airbyte_instance #, AirbyteResource

from r_s.assets import orig_movies, orig_users, orig_scores
from r_s.assets import training_data
from r_s.assets import preprocessed_data,split_data,model_trained,log_model,model_metrics

from r_s.configs import job_data_config, job_training_config

import os
from dotenv import load_dotenv
load_dotenv() 

# Crear dos grupos de assets
core_assets = [orig_movies, orig_users, orig_scores]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]
dbt_group = [dbt_users,dbt_movies,dbt_scores,my_dbt_assets]


from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance

airbyte_resource = AirbyteResource(
    host=os.getenv("AIRBYTE_HOST"),
    port=os.getenv("AIRBYTE_PORT"),
    # If using basic auth
    username=os.getenv("AIRBYTE_USER"),
    password=os.getenv("AIRBYTE_PASSWORD"),
)

airbyte_assets = load_assets_from_airbyte_instance(
   airbyte_resource,
   key_prefix=["ab_"]
   )

airbyte_group = [airbyte_assets]
# Combine asset lists
all_assets = core_assets + recommender_assets + [airbyte_assets] + dbt_group

@job    #(config={"ops": {"my_dbt_assets": {"config": {"upstream": [airbyte_assets]}}}})
def my_data_pipeline():
    # for asset in airbyte_assets:
    #     asset()
    airbyte_assets
    my_dbt_assets()

# Define jobs with the selections
data_job = define_asset_job(
    name='get_data',
    selection=core_assets,
    #selection=AssetSelection.groups('core'),
    config=job_data_config
)

only_training_job = define_asset_job(
    name="only_training",
    selection=recommender_assets,
    #selection=AssetSelection.groups('recommender'),
    config=job_training_config
)

#------------------------------------------------------------------------------------------------
#                                     DEFINITIONS
#------------------------------------------------------------------------------------------------
dbt_manifest_path = os.getenv("DBT_MANIFEST_PATH")
# Combinar los assets de ambos grupos y definir el objeto Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        #my_data_pipeline,
        data_job,
        only_training_job
    ],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir),
                                    ),
               },
)