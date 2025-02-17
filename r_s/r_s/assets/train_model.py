from dagster import asset, AssetIn, Int, Float, multi_asset, AssetOut, Field
import pandas as pd
from dagster_mlflow import mlflow_tracking
from sklearn.model_selection import train_test_split

@multi_asset(
    group_name="recommender",
    ins={
        "training_data": AssetIn(
        )
    },
    outs={
        "preprocessed_training_data": AssetOut(),
        "user2Idx": AssetOut(),
        "movie2Idx": AssetOut(),
    }
)
def preprocessed_data(training_data: pd.DataFrame):
    u_unique = training_data.user_id.unique()
    user2Idx = {o:i+1 for i,o in enumerate(u_unique)}
    m_unique = training_data.movie_id.unique()
    movie2Idx = {o:i+1 for i,o in enumerate(m_unique)}
    training_data['encoded_user_id'] = training_data.user_id.apply(lambda x: user2Idx[x])
    training_data['encoded_movie_id'] = training_data.movie_id.apply(lambda x: movie2Idx[x])
    
    preprocessed_training_data = training_data.copy()

    return preprocessed_training_data, user2Idx, movie2Idx


@multi_asset(
    group_name="recommender",
    ins={
        "preprocessed_training_data": AssetIn(
    )
    },
    outs={
        "X_train": AssetOut(),
        "X_test": AssetOut(),
        "y_train": AssetOut(),
        "y_test": AssetOut(),
    }
)
def split_data(context, preprocessed_training_data):
    test_size=0.10
    random_state=42
    X_train, X_test, y_train, y_test = train_test_split(
        preprocessed_training_data[['encoded_user_id', 'encoded_movie_id']],
        preprocessed_training_data[['rating']],
        test_size=test_size, random_state=random_state
    )
    return X_train, X_test, y_train, y_test
    

@asset(
    group_name="recommender",
    required_resource_keys={"mlflow"},
    ins={
        "X_train": AssetIn(),
        "y_train": AssetIn(),
        "user2Idx": AssetIn(),
        "movie2Idx": AssetIn(),
    },
    config_schema={
        'batch_size': Field(Int, default_value=128),
        'epochs': Field(Int, default_value=10),
        'learning_rate': Field(Float, default_value=1e-3),
        'embeddings_dim': Field(Int, default_value=5)
    }
)
def model_trained(context, X_train, y_train, user2Idx, movie2Idx):
    from .model_helper import get_model
    from keras.optimizers import Adam

    mlflow = context.resources.mlflow
    mlflow.log_params(context.op_config)
    #mlflow.tensorflow.autolog()
    
    batch_size = context.op_config["batch_size"]
    epochs = context.op_config["epochs"]
    learning_rate = context.op_config["learning_rate"]
    embeddings_dim = context.op_config["embeddings_dim"]

    model = get_model(len(movie2Idx), len(user2Idx), embeddings_dim)
    model.compile(Adam(learning_rate=learning_rate), 'mean_squared_error')
    
    context.log.info(f'batch_size: {batch_size} - epochs: {epochs}')
    history = model.fit(
        [
            X_train.encoded_user_id,
            X_train.encoded_movie_id
        ], 
        y_train.rating, 
        batch_size=batch_size,
        # validation_data=([ratings_val.userId, ratings_val.movieId], ratings_val.rating), 
        epochs=epochs, 
        verbose=1
    )
    for i, l in enumerate(history.history['loss']):
        mlflow.log_metric('mse', l, i)

    from matplotlib import pyplot as plt
    fig, axs = plt.subplots(1)
    axs.plot(history.history['loss'], label='mse')
    plt.legend()
    mlflow.log_figure(fig, 'plots/loss.png')

    return model

@asset(
    group_name="recommender",
    required_resource_keys={"mlflow"},
    ins={
        "model_trained": AssetIn(),
    },
    name="model_stored"
)
def log_model(context, model_trained):
    import numpy as np
    mlflow = context.resources.mlflow
    
    logged_model = mlflow.tensorflow.log_model(
        model_trained,
        "keras_dot_product_model",
        registered_model_name='keras_dot_product_model',
        # input_example=[np.array([1, 2]), np.array([2, 3])],
    )
    # logged_model.flavors
    model_data = {
        'model_uri': logged_model.model_uri,
        'run_id': logged_model.run_id
    }
    return model_data


@asset(
    group_name="recommender",
    #resource_defs={'mlflow': mlflow_tracking},
    required_resource_keys={"mlflow"},
    ins={
        "model_stored": AssetIn(),
        "X_test": AssetIn(),
        "y_test": AssetIn(),
    }
)
def model_metrics(context, model_stored, X_test, y_test):
    mlflow = context.resources.mlflow
    logged_model = model_stored['model_uri']

    loaded_model = mlflow.pyfunc.load_model(logged_model)
    
    y_pred = loaded_model.predict([
            X_test.encoded_user_id,
            X_test.encoded_movie_id
    ])
    from sklearn.metrics import mean_squared_error

    mse = mean_squared_error(y_pred.reshape(-1), y_test.rating.values)
    metrics = {
        'test_mse': mse,
        'test_rmse': mse**(0.5)
    }
    mlflow.log_metrics(metrics)
    return metrics



    
    