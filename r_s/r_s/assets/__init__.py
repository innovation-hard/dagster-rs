##from .movies_users import movies, users, scores, training_data
from dagster import load_assets_from_package_module
from . import core
from . import recommender

# Define core_assets y recommender_assets
## core_assets = [movies, users, scores]
## recommender_assets = [training_data]

core_assets=load_assets_from_package_module(
    package_module=core,group_name='core'
)

recommenter_assets=load_assets_from_package_module(
    package_module=recommender,group_name='recommender'
)
