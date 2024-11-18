import setuptools
import os
DAGSTER_VERSION=os.getenv('DAGSTER_VERSION', '1.9.2')
DAGSTER_LIBS_VERSION=os.getenv('DAGSTER_LIBS_VERSION', '0.25.1')
MLFLOW_VERSION=os.getenv('MLFLOW_VERSION', '2.17.2')

setuptools.setup(
   name="movies-rs-dagster",
   packages=setuptools.find_packages(),
   install_requires=[
       f"dagster=={DAGSTER_VERSION}",
       f"dagster-gcp=={DAGSTER_LIBS_VERSION}",
       f"dagster-mlflow=={DAGSTER_LIBS_VERSION}",
       f"mlflow=={MLFLOW_VERSION}",
       f"tensorflow",
   ],
   extras_require={"dev": ["dagster-webserver", "pytest", "jupyter"], "tests": ["mypy", "pylint", "pytest"]},
)

