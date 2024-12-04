from dagster import Definitions, load_assets_from_modules, define_asset_job, load_assets_from_package_module
from .configs import job_data_config,data_ops_config

from r_s import assets

from dagster_mlflow import mlflow_tracking

mlflow_resource = mlflow_tracking.configured({
    "experiment_name": "recommender_system"
})


all_assets = load_assets_from_package_module(
	package_module=assets, group_name='core'
)

data_job = define_asset_job(
	name='get_data',
	selection=['movies','users','scores','training_data'],
	config=job_data_config
)

defs = Definitions(
    assets=all_assets,
    resources={
        "mlflow": mlflow_resource,
    },
    jobs=[data_job]
)

# Define el job asociando la configuración
movies_job = define_asset_job(
    "movies_job",
    config=data_ops_config
)
