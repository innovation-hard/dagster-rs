mlflow_resources = {
    'mlflow': {
        'config': {
            'experiment_name': 'r_s',
        }            
    },
}

data_config = {
    'movies': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'
        }
    },
    'users': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/usuarios_0.csv'
        }
    },
    'scores': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/scores_0.csv'
        }
    }
}


job_data_config = {
    'resources':   {
        **mlflow_resources
    },
    'ops':  {
        **data_config
    }
}

training_config = {
    "training_data": { 
        "config": {
            "model_trained": True,
            "batch_size": 128,
            "epochs": 10,
            "learning_rate": 1e-3,
            "embeddings_dim": 5
        },
    },
    "model_trained": { 
        "config": {
            "batch_size": 128,
            "epochs": 10,
            "learning_rate": 1e-3,
            "embeddings_dim": 5
        },
    }    
}

job_training_config = {
    'resources': {
        **mlflow_resources
    },
    'ops': {
        **training_config
    }
}
