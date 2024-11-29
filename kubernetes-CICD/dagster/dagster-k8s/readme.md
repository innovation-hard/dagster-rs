
Esto supone que ya se pusheo y que existe el container creado por gitlab

```
export CONTAINER_IMAGE=gcr.io/infoxel-tagx/ml/dagster-code/ner:latest
docker pull $CONTAINER_IMAGE
```

## Entrar
```
docker run --env-file .env --rm -it -p 3000:3000 -v /code:/home/code/ $CONTAINER_IMAGE /bin/bash
```

# Correr Dagster
```
docker run --env-file .env --rm -it -p 3000:3000 -v /code:/home/code/ $CONTAINER_IMAGE /bin/bash -c "dagit --host 0.0.0.0"
```

# Correr modelo
Obtener modelo guardado en MLFLOW en paso anterior


```
export MODEL_URI=models:/SeenkaNER-v2/77

docker run --rm -it -p 5000:5000 -v $GOOGLE_APPLICATION_CREDENTIALS_PATH:/home/code/ --env-file .env $IMAGE_NAME  /bin/bash -c "mlflow models serve -m $MODEL_URI  --host 0.0.0.0 --port 5000 --env-manager=local"
```