mlflow_resources = {
    'mlflow': {
        'config': {
            'experiment_name': 'r_s',
        }            
    },
}


from dagster_mlflow import mlflow_tracking

mlflow_resource = mlflow_tracking.configured({
    "experiment_name": "r_s",
    #"tracking_uri": "http://localhost:8002", 
})


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

#data_job
job_data_config = {
    'resources': {
        'mlflow': {
            'config': {
                'experiment_name': 'r_s',
            }
        }
    },
    # 'ops': {
    #     **training_config
    # }
}

#only_training_job
job_training_config = {
    'resources': {
        'mlflow': {
            'config': {
                'experiment_name': 'r_s',
            }
        }
    },
    'ops': training_config
}
