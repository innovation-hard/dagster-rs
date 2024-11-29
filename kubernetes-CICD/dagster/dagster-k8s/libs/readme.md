# Desde la raiz del repo
```
export DAGSTER_CODE_IMAGE_NAME=gcr.io/infoxel-tagx/ml/libs/ner-v2
export DAGSTER_CODE_REPO_PATH=/ner
export DAGSTER_CODE_REPO_WORKSPACE_PATH=/ner/ner_v2/repository.py
export PATH_TO_DOCKERFILE=dagster-k8s/libs-v2
```

```
docker build --tag $DAGSTER_CODE_IMAGE_NAME --build-arg repo_path=$DAGSTER_CODE_REPO_PATH -f $PATH_TO_DOCKERFILE/Dockerfile .
```

Testearlo
```
docker run --env-file $PATH_TO_DOCKERFILE/.env --rm -it -p 3000:3000 -v /code:/home/code/ $DAGSTER_CODE_IMAGE_NAME /bin/bash -c "dagit --python-file $DAGSTER_CODE_REPO_WORKSPACE_PATH --host 0.0.0.0 --working-directory $DAGSTER_CODE_REPO_PATH"
```
Entrar
```
docker run --env-file $PATH_TO_DOCKERFILE/.env --rm -it -p 3000:3000 -v /code:/home/code/ $DAGSTER_CODE_IMAGE_NAME /bin/bash

```
docker push $DAGSTER_CODE_IMAGE_NAME
```
