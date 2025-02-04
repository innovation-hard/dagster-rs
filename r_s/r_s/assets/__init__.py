from .movies_users import orig_movies, orig_users, orig_scores
from .training import training_data
from .train_model import preprocessed_data,split_data,model_trained,log_model ,model_metrics
#from r_s.assets.airbyte import airbyte_assets

# Define core_assets y recommender_assets
core_assets = [orig_movies, orig_users, orig_scores]
recommender_assets = [training_data,preprocessed_data,split_data,model_trained,log_model,model_metrics]