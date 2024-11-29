```
export DAGSTER_CODE_IMAGE_NAME=gcr.io/infoxel-tagx/ml/dagster-code/ner
export DAGSTER_CODE_REPO_URI=git@gitlab.com:infoxel/ml/ner.git
export DAGSTER_CODE_REPO_PATH=/ner
export DAGSTER_CODE_REPO_WORKSPACE_PATH=/ner/ner_v2/repository.py
```

```
docker build --no-cache --tag $DAGSTER_CODE_IMAGE_NAME --build-arg ssh_prv_key="$(cat ~/.ssh/id_rsa)" --build-arg ssh_pub_key="$(cat ~/.ssh/id_rsa.pub)" --build-arg repo_uri=$DAGSTER_CODE_REPO_URI --build-arg repo_path=$DAGSTER_CODE_REPO_PATH .
```
Testearlo
```
docker run --rm -it -p 3000:3000 -v /Users/julian/Documents/infoxel/code:/home/code/ --env-file .env $DAGSTER_CODE_IMAGE_NAME /bin/bash -c "dagit --python-file $DAGSTER_CODE_REPO_WORKSPACE_PATH --host 0.0.0.0 --working-directory $DAGSTER_CODE_REPO_PATH"

docker run --rm -it -p 3000:3000 -v /Users/julian/Documents/infoxel/code:/home/code/ $DAGSTER_CODE_IMAGE_NAME /bin/bash -c "dagit --python-file $DAGSTER_CODE_REPO_WORKSPACE_PATH --host 0.0.0.0 --working-directory $DAGSTER_CODE_REPO_PATH"

docker run --rm -it -p 3000:3000 -v /Users/julian/Documents/infoxel/code:/home/code/ --env-file .env $DAGSTER_CODE_IMAGE_NAME /bin/bash

```
docker push $DAGSTER_CODE_IMAGE_NAME
```
