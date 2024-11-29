FROM python:3.9-slim
RUN apt-get update && apt-get install libgl1 -y && apt-get install curl -y && apt-get install git -y && apt-get install build-essential -y
RUN curl https://pyenv.run | bash
RUN python -m  pip install virtualenv
ENV PATH="/root/.pyenv/bin:$PATH"
RUN python -m pip install mlflow==2.8