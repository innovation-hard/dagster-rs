from dagster import Definitions, AssetSelection #, ScheduleDefinition
from dagster import define_asset_job, AssetKey

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

from r_s.configs import job_data_config, job_training_config, mlflow_resource

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
#airbyte_group = [airbyte_assets]
airbyte_assets = load_assets_from_airbyte_instance(
    airbyte_resource,
    key_prefix=["ab_"]
)

core_assets = [orig_movies, orig_users, orig_scores]
dbt_group = [my_dbt_assets]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]

all_assets = [airbyte_assets] + core_assets + dbt_group + recommender_assets

#------------------------------------------------------------------------------------------------
#                                     JOBS
#------------------------------------------------------------------------------------------------
data_job = define_asset_job(
    name='get_data',
    selection=AssetSelection.keys(AssetKey(["ab_", "movies"]), AssetKey(["ab_", "users"]), AssetKey(["ab_", "scores"]), "orig_movies", "orig_users", "orig_scores"),
    config=job_data_config
)

only_training_job = define_asset_job(
    name="only_training",
    #selection=recommender_assets,
    #selection=AssetSelection.groups("my_dbt_assets") | AssetSelection.groups('recommender'),
    #selection=AssetSelection.keys("my_dbt_asset") | AssetSelection.keys("training_data", "preprocessed_data", "split_data", "model_trained", "log_model", "model_metrics"),
    # selection=AssetSelection.keys(
    # "movies", "users", "scores",  # Assets de my_dbt_assets
    # "training_data", "preprocessed_data", "split_data", "model_trained", "log_model", "model_metrics"  # Assets de recommender
    # ),
    selection=AssetSelection.all() - AssetSelection.keys(AssetKey(["ab_", "movies"]), AssetKey(["ab_", "users"]), AssetKey(["ab_", "scores"]), "orig_movies", "orig_users", "orig_scores"),    
    config=job_training_config
)

order_pipeline_job = define_asset_job(
    name="order_pipeline_job",
    selection=AssetSelection.groups("core") | AssetSelection.groups("my_dbt_assets"),
)

# Define a job that runs the entire pipeline
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=AssetSelection.all(),
)

#------------------------------------------------------------------------------------------------
#                                     SCHEDULE
#------------------------------------------------------------------------------------------------
from dagster import schedule

@schedule(
    job=data_job,
    cron_schedule="*/30 * * * *",  # Ejecutar cada 24 horas
)
def hourly_sensor_schedule(context):
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
import uuid

@sensor(job=only_training_job,minimum_interval_seconds=120)
def postgres_change_sensor(context):
    """ 
    Sensor que verifica cambios en PostgreSQL y ejecuta el pipeline si los detecta.
    """
    if check_for_changes_in_postgres():
        context.log.info("Sensor: Cambios detectados en PostgreSQL. Ejecutando pipeline...")
        
        # Generar un run_key único (por ejemplo, un UUID)
        run_key = f"postgres_change_run_{uuid.uuid4()}"
        
        run_config = {
            # configuración específica only_training_job
        }
        
        return RunRequest(run_key=run_key, run_config=run_config)
    else:
        context.log.info("Sensor: No se detectaron cambios en PostgreSQL.")
        return SkipReason("No se detectaron cambios en PostgreSQL.")


from dagster import sensor, RunRequest, SkipReason, RunsFilter

@sensor(job=only_training_job)
def data_job_sensor(context):
    # Obtener el cursor almacenado (último ID de ejecución procesado)
    last_processed_run_id = context.cursor or None

    # Obtener la última ejecución de data_job usando get_run_records
    run_records = context.instance.get_run_records(
        filters=RunsFilter(job_name="data_job"),
        limit=1,
        order_by="update_timestamp DESC",  # Usar una cadena, no una lista
    )

    # Si no hay ejecuciones, salir
    if not run_records:
        return SkipReason("No hay ejecuciones de data_job.")

    latest_run = run_records[0].dagster_run

    # Verificar si la última ejecución es nueva (no procesada por el sensor)
    if last_processed_run_id == latest_run.run_id:
        return SkipReason("La última ejecución de data_job ya fue procesada.")

    # Verificar si la última ejecución fue exitosa
    if latest_run.status == "SUCCESS":
        # Actualizar el cursor con el ID de la ejecución actual
        context.update_cursor(latest_run.run_id)
        return RunRequest(run_key="only_training_job")
    else:
        return SkipReason("La última ejecución de data_job no fue exitosa.")
    
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
        order_pipeline_job,
        full_pipeline_job
    ],
    sensors=[postgres_change_sensor],
    schedules=[hourly_schedule,hourly_sensor_schedule],
    resources={"dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
               "postgres": postgres_db,
                "mlflow": mlflow_resource,
               },
)