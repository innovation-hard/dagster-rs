
```
export IMAGE_NAME=gcr.io/infoxel-tagx/ml/models/triton/ner
export GOOGLE_APPLICATION_CREDENTIALS_PATH=/code
```

```
docker build --no-cache --tag $IMAGE_NAME  .
docker build --tag $IMAGE_NAME  .
```
# Testearlo sin GPU (en mi maquina tira signal 9 por memoria)
```
1)
docker run --rm -it -p 5000:5000 -v $GOOGLE_APPLICATION_CREDENTIALS_PATH:/home/code/ --env-file .env $IMAGE_NAME  /bin/bash

2) Correr tunel a tritón

gcloud container clusters get-credentials tagx-2 --zone us-central1-a --project infoxel-tagx && kubectl port-forward $(kubectl get pod --selector="app=ml-triton-test" --output jsonpath='{.items[0].metadata.name}')

3) Abrir una nueva terminal y hacer 

docker ps
docker exec -it 12a226777fae /bin/bash (reemplazar el container id)

4) Instanciar el modelo
 mlflow models serve -m models:/triton-ner/8  --host 0.0.0.0 --port 5000 --env-manager=local  (reemplazar la version)

5) Testear el modelo 

import requests
response = requests.post(
    'http://localhost:5000/invocations',
    json={"dataframe_records": df.to_dict(orient="records")}
)
```
# Solo entrar a VM
```
docker run --rm -it -p 5000:5000 -v $GOOGLE_APPLICATION_CREDENTIALS_PATH:/home/code/ --env-file .env $IMAGE_NAME  /bin/bash
```

# Testearlo con GPU
```
docker run --rm -it -p 5000:5000 -v $GOOGLE_APPLICATION_CREDENTIALS_PATH:/home/code/ --env-file .env --gpus all -e NVIDIA_DRIVER_CAPABILITIES=compute,utility,video -e NVIDIA_VISIBLE_DEVICES=all $IMAGE_NAME  /bin/bash -c "mlflow models serve -m models:/SeenkaNER-v2/44  --host 0.0.0.0 --port 5000 --env-manager=local"


```
docker push $IMAGE_NAME
```
