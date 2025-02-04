 Creación de entorno
```bash

conda create -n dagster-mlops-rs 
# python=3.9
conda config --add channels conda-forge
conda activate dagster-mlops-rs

# El problema que tuve resultó en que terminé teniendo python=3.13, ahora
# instalé python=3.10 (que tampoco es el sugerido, a ver..)
# conda install python=3.12
conda install dagster=1.9.1

# Creo estructura de carpetas
dagster project scaffold --name rec_sys

```

# Instalación de dependencias y creación de paquete
Modificar el archivo de setup para agregar las librerias correspondientes

```python
from setuptools import find_packages, setup
import os

DAGSTER_VERSION=os.getenv('DAGSTER_VERSION', '1.9.2')
DAGSTER_LIBS_VERSION=os.getenv('DAGSTER_LIBS_VERSION', '0.21.6')
MLFLOW_VERSION=os.getenv('MLFLOW_VERSION', '2.8.0')

setup(
    name="recommender_system",
    packages=find_packages(exclude=["recommender_system_tests"]),
    install_requires=[
        f"dagster=={DAGSTER_VERSION}",
        f"dagster-mlflow=={DAGSTER_LIBS_VERSION}",
        f"mlflow=={MLFLOW_VERSION}",
        f"tensorflow==2.14.0",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "jupyter"]},
)
```


```bash
cd rec_sys
pip install -e ".[dev]"
```

# Correr dagster en modo development
```bash
# Seteo de variables Windows
set -o allexport && source environments/local && set +o allexport
# Linux
set -a && source environments/local && set +a
```

# Correr mlflow (mirar repo clase anterior)

```bash

# va a ser necessario para los assets
pip install dagster_mlflow

export MLFLOW_TRACKING_URI=http://localhost:5000

# El profe no metió este comando todavia
mlflow server --backend-store-uri sqlite:///mydb.sqlite

# Opción prefijo: python -m mlflow server

mlflow server --backend-store-uri postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$MLFLOW_POSTGRES_DB --default-artifact-root $MLFLOW_ARTIFACTS_PATH --host 0.0.0.0 -p 5000 


# Para persistir la data
export DAGSTER_HOME=/home/repo/ML/dagster-rs/dagster_home


# carpeta assets
mkdir assets

# Corro dagster
dagster dev -d /home/repo/ML/dagster-rs/r_s
dagster dev
```

# Archivos `__init__.py`

En la raiz:
Cambio éste:

```python 
from dagster import Definitions, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)
```

Por éste:

```python 
# Importo assets
from dagster import Definitions, define_asset_job
from .assets import (
    core_assets, recommender_assets
)

all_assets = [*core_assets, *recommender_assets]

# Configuraciones de jobs (Podria ir aparte y la importo)
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

# Definiciones
defs = Definitions(
    assets=all_assets,
    jobs=[define_asset_job("train_full_model", config=job_configs)],
    resources={'mlflow': mlflow_tracking},
    schedules=[core_assets_schedule],
    sensors=all_sensors,
)
```

En la carpeta assets:
```python
from dagster import load_assets_from_package_module
from . import core
from . import recommender

core_assets = load_assets_from_package_module(package_module=core, group_name='core')
recommender_assets = load_assets_from_package_module(package_module=recommender, group_name='recommender')
```

Recordar agregar los archivos `__init__.py` en todas las carpetas que quiero que formen parte del paquete


# Test
pytest --disable-warnings


# Buil mlflow docker

```bash
docker build --tag base-mlflow:2.8 .

docker run --network host -p 5001:5001 -it base-mlflow:2.8 bash 

# windows o mac 
export MLFLOW_TRACKING_URI=http://host.docker.internal:5000
# linux
export MLFLOW_TRACKING_URI=http://localhost:5000

curl $MLFLOW_TRACKING_URI

mlflow models serve -m models:/keras_dot_product_model/1 --env-manager=conda --port 5001
```
