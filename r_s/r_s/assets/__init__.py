from .movies_users import movies, users, scores, training_data
from .train_model import preprocessed_data,split_data,model_trained,log_model ,model_metrics


# Define core_assets y recommender_assets
core_assets = [movies, users, scores]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]