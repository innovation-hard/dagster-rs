```bash
docker build --tag base-mlflow-pyenv:2.8 .

docker run --network host -p 5001:5001 -it base-mlflow-pyenv:2.8 bash 

# windows o mac 
export MLFLOW_TRACKING_URI=http://host.docker.internal:5000
# linux
export MLFLOW_TRACKING_URI=http://localhost:5000

curl $MLFLOW_TRACKING_URI

mlflow models serve -m models:/keras_dot_product_model/1 --port 5001