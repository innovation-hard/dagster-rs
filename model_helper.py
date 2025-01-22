def get_model(n_movies, n_users, n_latent_factors):
    from tensorflow.python.keras.layers import Input, Embedding, Flatten, Dot

    #from tensorflow.python import layers
    from tensorflow.python.keras.models import Model
    
    movie_input = Input(shape=[1], name='Item')
    user_input = Input(shape=[1],name='User')
    
    movie_embedding = Embedding(
        n_movies + 1, n_latent_factors, 
        mask_zero=True,
        name='Movie-Embedding'
    )(movie_input)
    movie_vec = Flatten(name='FlattenMovies')(movie_embedding)

    user_embedding = Embedding(
            n_users + 1,
            n_latent_factors,
            mask_zero=True,
            name='User-Embedding'
    )(user_input)
    user_vec = Flatten(name='FlattenUsers')(user_embedding)

    prod = Dot(axes=1, name='DotProduct')([movie_vec, user_vec])
    model = Model([user_input, movie_input], prod)
    return model