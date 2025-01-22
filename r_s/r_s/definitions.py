from dagster import Definitions, define_asset_job
#from dagster import AssetSelection
#from dagster import load_assets_from_modules

from r_s.configs import job_data_config, job_training_config
from r_s.assets import movies, users, scores, training_data
#from r_s.assets import movies_users,train_model
from r_s.assets import preprocessed_data,split_data,model_trained,log_model,model_metrics

# Crear dos grupos de assets
core_assets = [movies, users, scores]
#core_assets = load_assets_from_modules(movies_users,group_name='core')
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]
#recommender_assets = load_assets_from_modules(train_model,group_name='recommender')

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

# Combinar los assets de ambos grupos y definir el objeto Definitions
defs = Definitions(
    assets=core_assets + recommender_assets,  # Combina ambos grupos
    jobs=[
        data_job,
        only_training_job
    ]
)
