FROM python:3.9-slim
RUN apt-get update && apt-get install libgl1 -y && apt-get install curl -y && apt-get install git -y
COPY requirements.txt requirements.txt
RUN python -m pip install -r requirements.txt
