from dagster_dbt import DbtCliResource, dbt_assets
from dagster import OpExecutionContext, asset

from r_s.constants import dbt_manifest_path

@dbt_assets(manifest=dbt_manifest_path)
def my_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

