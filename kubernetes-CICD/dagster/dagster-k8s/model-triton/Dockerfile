FROM gcr.io/infoxel-tagx/gcloud-python-3.8

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y myspell-es \
    && apt-get install -y libenchant-2-dev

RUN pip install mlflow==2.3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt