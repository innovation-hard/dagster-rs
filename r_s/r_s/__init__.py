#__init__.py

from r_s.assets import movies, users, scores, training_data

##from dagster import load_assets_from_modules
##from . import core
##from . import recommender

##core_assets=load_assets_from_modules(
##    package_module=core,group_name='core'
##)

##recommenter_assets=load_assets_from_modules(
##    package_module=recommender,group_name='recommender'
##)

## Define core_assets y recommender_assets
core_assets = [movies, users, scores]
recommender_assets = [training_data]