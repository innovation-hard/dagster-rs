from dagster import Definitions, define_asset_job
from dagster_mlflow import mlflow_tracking

from .assets import core_assets, recommender_assets

all_assets = [*core_assets, *recommender_assets]

mlflow_resource = mlflow_tracking.configured({
    'experiment_name': 'recommender_system'
})

# Configuraciones de jobs
job_configs = {
    'resources': {
        'mlflow': {
            'config': {
                'experiment_name': 'recommender_system',
            }            
        },
    },
    'ops': {
        'movies': {
            'config': {
                'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'
            }
        },
        'keras_dot_product_model': {'config': {
            'batch_size': 128,
            'epochs': 10,
            'learning_rate': 1e-3,
            'embeddings_dim': 5
        }}
    }
}


