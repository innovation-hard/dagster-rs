from dagster import Definitions, define_asset_job, AssetSelection
from r_s.configs import job_data_config, job_training_config
from r_s.assets import movies, users, scores, training_data

# Crear dos grupos de assets
core_assets = [movies, users, scores]  # Assets del grupo 'core'
recommender_assets = [training_data]  # Asset del grupo 'recommender'

# Define jobs with the selections
data_job = define_asset_job(
    name='get_data',
    selection=core_assets,  # Use the defined asset selection
    config=job_data_config
)


only_training_job = define_asset_job(
    name="only_training",
    selection=recommender_assets,  # Assets del grupo 'recommender'
    config=job_training_config
)

# Combinar los assets de ambos grupos y definir el objeto Definitions
defs = Definitions(
    assets=core_assets + recommender_assets,  # Combina ambos grupos
    jobs=[
        data_job,
        only_training_job
    ]
)
