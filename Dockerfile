# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.10-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED=1

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN apt update && apt install -y git

# Install production dependencies.
RUN pip3 install --no-cache-dir -r requirements.txt

RUN pip3 install opentelemetry-distro opentelemetry-exporter-otlp
RUN opentelemetry-bootstrap -a install

ENTRYPOINT ["opentelemetry-instrument", "python", "main.py"]
# ENTRYPOINT ["python", "main.py"]