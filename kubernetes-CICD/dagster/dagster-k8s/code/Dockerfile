FROM gcr.io/infoxel-tagx/ml/libs/ner

ARG ssh_pub_key
ARG ssh_prv_key
ARG repo_uri
ARG repo_path

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan gitlab.com > /root/.ssh/known_hosts

# Add the keys and set permissions
RUN echo "$ssh_prv_key" > /root/.ssh/id_rsa && \
    echo "$ssh_pub_key" > /root/.ssh/id_rsa.pub && \
    chmod 600 /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa.pub

RUN rm -rf /ner
RUN git clone ${repo_uri}
RUN rm -rf /dagster-model-testing-and-deploy
RUN git clone git@gitlab.com:infoxel/ml/dagster-model-testing-and-deploy.git
RUN rm -rf /mlflow-dagster-model
RUN git clone git@gitlab.com:infoxel/ml/mlflow-dagster-model.git

RUN rm /root/.ssh/id_rsa && \
    rm /root/.ssh/id_rsa.pub


