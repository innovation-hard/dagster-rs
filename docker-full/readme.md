```bash
docker build --tag model-rec-sys:latest .

docker run --env MLFLOW_TRACKING_URI=http://host.docker.internal:5000 -p 5001:5001 -it model-rec-sys:latest bash 

curl $MLFLOW_TRACKING_URI

mlflow models serve -m models:/keras_dot_product_model/1 --host 0.0.0.0 --port 5001 --env-manager=local



docker run --env MLFLOW_TRACKING_URI=http://host.docker.internal:5000 -p 5001:5001 -it gcs://mlmodels/model-rec-sys:latest /bin/bash -c 'mlflow models serve -m models:/keras_dot_product_model/1 --host 0.0.0.0 --port 5001 --env-manager=local'

docker ps

docker exec -it ID_DE_PS bash