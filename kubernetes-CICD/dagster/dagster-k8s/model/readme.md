Este build lo tuve que correr en la vm con GPU y antes hacer un docker system prune. Es conveniente tambien borrar las imagenes de docker que ya existan. 
Es muy pesado y en mi máquina no corría

gcloud compute ssh --ssh-flag="-L 5000:localhost:5000" --zone "us-central1-a" "ml-gpu-t4-hm" --project "infoxel-tagx"

```
export IMAGE_NAME=gcr.io/infoxel-tagx/ml/models/ner-v2
export GOOGLE_APPLICATION_CREDENTIALS_PATH=/code
```

```
docker build --no-cache --tag $IMAGE_NAME  .
docker build --tag $IMAGE_NAME  .
```
# Testearlo sin GPU (en mi maquina tira signal 9 por memoria)
```
docker run --rm -it -p 5000:5000 -v $GOOGLE_APPLICATION_CREDENTIALS_PATH:/home/code/ --env-file .env $IMAGE_NAME  /bin/bash -c "mlflow models serve -m models:/SeenkaNER-v2/85  --host 0.0.0.0 --port 5000 --env-manager=local"
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
