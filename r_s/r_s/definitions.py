from dagster import Definitions, define_asset_job, AssetSelection
from r_s.configs import job_data_config, job_training_config
from r_s.assets import movies, users, scores, training_data

# Crear dos grupos de assets
core_assets = [movies, users, scores]  # Assets del grupo 'core'
recommender_assets = [training_data]  # Asset del grupo 'recommender'

# Definir jobs para materializar los grupos
data_job = define_asset_job(
    name='get_data',
    selection=['movies', 'users', 'scores'],
    config=job_data_config
)

only_training_job = define_asset_job(
    name="only_training",
    selection=AssetSelection.groups('recommender'),  # Assets del grupo 'recommender'
    config=job_training_config
)

# Combinar los assets de ambos grupos y definir el objeto Definitions
defs = Definitions(
    assets=core_assets + recommender_assets,  # Combina ambos grupos
    jobs=[
        define_asset_job(
            name='get_data',
            selection=['movies', 'users', 'scores'],  # Solo core
            config=job_data_config
        ),
        define_asset_job(
            name='only_training',
            selection=['training_data'],  # Solo training_data
            config=job_training_config
        )
    ]
)
