from dagster import Definitions, AssetSelection #, ScheduleDefinition
from dagster import define_asset_job    #,job, AssetSelection, 

from dagster_dbt import DbtCliResource
import sys
#sys.path.append(os.path.abspath(os.path.dirname(__file__)))
sys.path.append("/home/repo/ML/dagster-rs/r_s")

from r_s.assets.dbt import my_dbt_assets
from r_s.constants import dbt_project_dir

from dagster_airbyte import airbyte_resource,load_assets_from_airbyte_instance #, AirbyteResource

from r_s.assets import orig_movies, orig_users, orig_scores
from r_s.assets import training_data
from r_s.assets import preprocessed_data,split_data,model_trained,log_model,model_metrics

from r_s.configs import job_data_config, job_training_config, mlflow_resource, training_config

import os
from dotenv import load_dotenv
load_dotenv('/home/repo/ML/dagster-rs/r_s/r_s/.env') 

#------------------------------------------------------------------------------------------------
#                                     POSTGRESQL
#------------------------------------------------------------------------------------------------
# Antes (causa error)
#from dagster_postgres import postgres_resource  

# Configuración del recurso de PostgreSQL
# postgres_config = {
#     "host": os.getenv("POSTGRES_HOST"),
#     "port": os.getenv("POSTGRES_PORT"),
#     "database": os.getenv("POSTGRES_DB"),
#     "user": os.getenv("POSTGRES_USER"),
#     "password": os.getenv("POSTGRES_PASSWORD"),
# }

# postgres_resource_configured = PostgresResource.configured(postgres_config)

#----------------------
# Ahora (versión actual)
# from dagster_postgres import PostgresResource

# postgres_db = PostgresResource(
#     username=os.getenv("POSTGRES_USER"),
#     password=os.getenv("POSTGRES_PASSWORD"),
#     hostname=os.getenv("POSTGRES_HOST"),
#     port= os.getenv("POSTGRES_PORT"),
#     db_name=os.getenv("POSTGRES_DB"),
# )

#------------------------------------------------
from dagster import ConfigurableResource
import psycopg2

class CustomPostgresResource(ConfigurableResource):
    username: str
    password: str
    hostname: str
    port: int
    db_name: str

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.username,
            password=self.password,
            host=self.hostname,
            port=self.port
        )

postgres_db = CustomPostgresResource(
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    hostname=os.getenv("POSTGRES_HOST"),
    port= os.getenv("POSTGRES_PORT"),
    db_name=os.getenv("POSTGRES_DB"),
)

# defs = Definitions(
#     assets=[...],
#     resources={"postgres": postgres_db},
#     jobs=[...]
# )


#------------------------------------------------------------------------------------------------
#                                     Assets Groups 1
#------------------------------------------------------------------------------------------------
# Crear dos grupos de assets
core_assets = [orig_movies, orig_users, orig_scores]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]
dbt_group = [my_dbt_assets]


#------------------------------------------------------------------------------------------------
#                                     AIRBYTE
#------------------------------------------------------------------------------------------------
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


#------------------------------------------------------------------------------------------------
#                                     Assets Groups 2
#------------------------------------------------------------------------------------------------
airbyte_group = [airbyte_assets]
# Combine asset lists
all_assets = core_assets + recommender_assets + airbyte_group + dbt_group

#------------------------------------------------------------------------------------------------
#                                     JOBS
#------------------------------------------------------------------------------------------------
# Define jobs with the selections
data_job = define_asset_job(
    name='get_data',
    selection=core_assets,
    #selection=AssetSelection.groups('core'),
    config=job_data_config
)

only_training_job = define_asset_job(
    name="only_training",
    #selection=recommender_assets,
    #selection=AssetSelection.keys("model_trained"),
    selection=AssetSelection.groups('recommender'),
    config=job_training_config
)

# Define a job that runs the entire pipeline
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=AssetSelection.all(),  # Selecciona todos los assets
)

#------------------------------------------------------------------------------------------------
#                                     DEFINITIONS
#------------------------------------------------------------------------------------------------
dbt_manifest_path = os.getenv("DBT_MANIFEST_PATH")
# Combinar los assets de ambos grupos y definir el objeto Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        data_job,
        only_training_job,
        full_pipeline_job
    ],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
               "postgres": postgres_db,
                #"postgres": postgres_resource_configured,
                "mlflow": mlflow_resource,
                #"training_data": training_config,
               },
)