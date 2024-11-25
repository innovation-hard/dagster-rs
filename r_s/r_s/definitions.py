from dagster import Definitions, load_assets_from_modules, define_asset_job, load_assets_from_package_module
from .configs import job_data_config

from r_s import assets

all_assets = load_assets_from_package_module(
	package_module=assets, group_name='core'
)

data_job = define_asset_job(
	name='get_data',
	selection=['movies','users','scores','training_data'],
	config=job_data_config
)

defs = Definitions(
	asset=all_assets,
	jobs=[data_job]
)