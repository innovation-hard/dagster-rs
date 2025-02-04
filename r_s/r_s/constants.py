from dagster_dbt import DbtCliResource
from dagster._utils import file_relative_path

import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

dbt_manifest_path = os.getenv("DBT_MANIFEST_PATH")

dbt_project_dir = Path(__file__).joinpath("..", "..", "..").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir)
                     )

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at runtime.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"], manifest={}).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

#
DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_CONFIG = {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}