FROM gcr.io/infoxel-tagx/gcloud-python-3.8

RUN apt-get update \
    && apt-get install -y git \
    && apt-get install -y myspell-es \
    && apt-get install -y libenchant-2-dev

ENV DAGSTER_VERSION=1.3.6
ENV DAGSTER_LIBS_VERSION=0.19.6
ENV MLFLOW_VERSION=2.3.2

ARG repo_path
COPY . ${repo_path}
WORKDIR ${repo_path}

RUN pip install .


