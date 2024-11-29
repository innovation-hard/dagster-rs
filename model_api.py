# %%
import requests
import pandas as pd
import mlflow
import numpy as np
# %%
loaded_model = mlflow.pyfunc.load_model('models:/keras_dot_product_model/1')
# %%
pd.DataFrame([[1, 2, 3], [4, 5, 6]])
# %%

loaded_model.predict(
    # [np.array([1, 2, 3]), np.array([4, 5, 6])]
    pd.DataFrame([[1, 2, 3], [4, 5, 6]])
)
# %%

# %%
json_data = {
    "instances": [
        {"User": [1], "Item": [1]},
        {"User": [2], "Item": [3]},
    ]
}
json_data
# %%
import json
json.dumps(json_data)
# %%
url = 'http://localhost:5001/invocations'

response = requests.post(
    url,
    json=json_data
)
# %%
response.text
# %% en docker
# mlflow models build-docker -m models:/NER_HF/4 -n ner-docker --env-manager conda --install-mlflow

# %%
curl http://127.0.0.1:5001/invocations -H 'Content-Type: application/json' -d '{
    "instances": 
        [
            {"User": [1], "Item": [1]},
            {"User": [2], "Item": [3]}
        ]
}'