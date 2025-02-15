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

from r_s.sensor import check_for_changes_in_postgres

import os
from dotenv import load_dotenv
load_dotenv('/home/repo/ML/dagster-rs/r_s/r_s/.env') 

#------------------------------------------------------------------------------------------------
#                                     POSTGRESQL
#------------------------------------------------------------------------------------------------
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

#------------------------------------------------------------------------------------------------
#                                     AIRBYTE
#------------------------------------------------------------------------------------------------
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance

airbyte_resource = AirbyteResource(
    host=os.getenv("AIRBYTE_HOST"),
    port=os.getenv("AIRBYTE_PORT"),
    username=os.getenv("AIRBYTE_USER"),
    password=os.getenv("AIRBYTE_PASSWORD"),
)

airbyte_assets = load_assets_from_airbyte_instance(
   airbyte_resource,
   key_prefix=["ab_"]
   )

#------------------------------------------------------------------------------------------------
#                                     Assets Groups
#------------------------------------------------------------------------------------------------
airbyte_group = [airbyte_assets]
core_assets = [orig_movies, orig_users, orig_scores]
dbt_group = [my_dbt_assets]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]

all_assets = airbyte_group + core_assets + dbt_group + recommender_assets

#------------------------------------------------------------------------------------------------
#                                     JOBS
#------------------------------------------------------------------------------------------------
data_job = define_asset_job(
    name='get_data',
    selection=core_assets,
    #selection=AssetSelection.groups('core'),
    config=job_data_config
)

only_training_job = define_asset_job(
    name="only_training",
    #selection=recommender_assets,
    selection=AssetSelection.groups('recommender'),
    config=job_training_config
)

# Define a job that runs the entire pipeline
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    # selection=(
    #     AssetSelection.keys(["ab_", "movies"]) |  # Assets de Airbyte
    #     AssetSelection.keys(["ab_", "scores"]) |
    #     AssetSelection.keys(["ab_", "users"]) |
    #     AssetSelection.keys("orig_movies") |  # Assets de core
    #     AssetSelection.keys("orig_users") |
    #     AssetSelection.keys("orig_scores") |
    #     AssetSelection.keys("my_dbt_assets") |  # Assets de dbt
    #     AssetSelection.keys("training_data")  # Assets de recommender
    # ),
    #selection=AssetSelection.keys(["ab_", "movies"]) | AssetSelection.keys(["ab_", "scores"]) | AssetSelection.keys(["ab_", "users"]) | AssetSelection.keys("orig_movies", "orig_users", "orig_scores") | AssetSelection.keys("my_dbt_assets") | AssetSelection.keys("training_data"),
    selection=AssetSelection.all(),
    #selection=AssetSelection.groups("core") | AssetSelection.groups("dbt") | AssetSelection.groups("recommender"),
    #selection=AssetSelection.groups("airbyte_group") | AssetSelection.groups("core") | AssetSelection.groups("dbt") | AssetSelection.groups("recommender"),
)

#------------------------------------------------------------------------------------------------
#                                     SCHEDULE
#------------------------------------------------------------------------------------------------
from dagster import schedule

@schedule(
    job=full_pipeline_job,
    cron_schedule="0 0 * * *",  # Ejecutar cada 24 horas
)
def daily_schedule(context):
    """
    Schedule que ejecuta el pipeline una vez al día como respaldo.
    """
    context.log.info("Corriendo daily_schedule...")
    return {}

@schedule(
    job=full_pipeline_job,
    cron_schedule="0 * * * *",  # Ejecutar cada 60 minutos
)
def hourly_schedule(context):
    """
    Schedule que ejecuta el pipeline una vez al día como respaldo.
    """
    context.log.info("Corriendo hourly_schedule...")
    return {}

#------------------------------------------------------------------------------------------------
#                                     SENSOR
#------------------------------------------------------------------------------------------------
from dagster import sensor, RunRequest

@sensor(job=full_pipeline_job)
def postgres_change_sensor(context):
    """ 
    Sensor que verifica cambios en PostgreSQL y ejecuta el pipeline si los detecta.
    """
    if check_for_changes_in_postgres():
        context.log.info("Sensor: Cambios detectados en PostgreSQL. Ejecutando pipeline...")
        return RunRequest(run_key="postgres_change_run", run_config={})
    else:
        context.log.info("Sensor: No se detectaron cambios en PostgreSQL.")
        return None


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
    sensors=[postgres_change_sensor],
    schedules=[hourly_schedule],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
               "postgres": postgres_db,
                #"postgres": postgres_resource_configured,
                "mlflow": mlflow_resource,
                #"training_data": training_config,
               },
)