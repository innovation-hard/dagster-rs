# %%
import mlflow
logged_model = 'models:/keras_dot_product_model/7'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)
# %%

# %%
# Predict on a Pandas DataFrame.
import numpy as np
loaded_model.predict([np.array([1, 2]), np.array([2, 3])])
# %%
# %%
mlflow.set_experiment('test')
# %%
# %%
mlflow.start_run()
# %%
from tensorflow.keras import layers, Model
# %%
input = layers.Input(shape=(1,))
dense = layers.Dense(10)(input)
model = Model(input, dense)
# %%
model.summary()
# %%
logged_model = mlflow.tensorflow.log_model(
        model,
        "simple",
        registered_model_name='simple'
    )
# %%
logged_model.__dir__()
# %%

# %%
{
    'model_uri': logged_model.model_uri,
    'run_id': logged_model.run_id
}
# %%
logged_model.flavors
# %%
from movies_rs_dagster.assets.recommender.train_model import log_model
# %%
from movies_rs_dagster.assets.core.movies_users import movies, users, scores
from dagster import build_op_context
from movies_rs_dagster import job_configs
from dagster_mlflow import mlflow_tracking
# %%
from movies_rs_dagster import defs
from dagster import ExecuteInProcessResult
# %%
get_data_job = defs.get_job_def('get_data')
result = get_data_job.execute_in_process()
# %%
assert isinstance(result, ExecuteInProcessResult)
assert result.success
data = result.output_for_node('training_data')
# %%
result.asset_observations_for_node('preprocessed_training_data')
# %%
data
# %%
result.
# %%
y_pred = loaded_model.predict([
            data.user_id[:10],
            data.movie_id[:10]
        ])
# %%
((data.rating[:10].values - y_pred.reshape(-1))**2).sum()/10
# %%
from sklearn.metrics import mean_squared_error
# %%
mean_squared_error(y_pred.reshape(-1), data.rating[:10].values)
# %%
# %%
# %%

# %%
result.asset_value('user2id')
# %%
result = get_data_job
# %%
job_configs['resources']
# %%
context = build_op_context(
    op_config={'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'}

)
# %%
users_out = users(context)
movies_out = movies(context)
# %%
scores(build_op_context(
    resources_config=job_configs['resources'],
    resources={'mlflow': mlflow_tracking}
))
# %%
# %%
users_out.value
# %%
# %%
job_configs['resources']

# %%
