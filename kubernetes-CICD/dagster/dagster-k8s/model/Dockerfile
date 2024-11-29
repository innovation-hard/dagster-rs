FROM gcr.io/infoxel-tagx/ml/models/base-gpu


ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y myspell-es
RUN apt-get install -y libenchant-dev

RUN pip install mlflow==2.3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY preload_models.py preload_models.py

RUN python3.8 preload_models.py
