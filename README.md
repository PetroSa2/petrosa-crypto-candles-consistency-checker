# Docker Build And Push GitHub Actions Workflow

This repository contains a GitHub Actions workflow for building and pushing a Docker image.

## Workflow Name

The workflow is named "Docker Build And Push".

## Trigger

The workflow is triggered on a push to the 'main' branch.

## Jobs

The workflow contains a single job named 'docker'. This job runs on the latest version of Ubuntu.

### Steps

The 'docker' job consists of the following steps:

1. **Set up QEMU:** This step uses the docker/setup-qemu-action@v2 to set up QEMU. QEMU is a generic and open source machine emulator and virtualizer.

2. **Set up Docker Buildx:** This step uses the docker/setup-buildx-action@v2 to set up Docker Buildx. Buildx is a Docker CLI plugin that extends the build capabilities of Docker with the full support of the features provided by Moby BuildKit builder toolkit.

3. **Login to DockerHub:** This step uses the docker/login-action@v2 to log in to DockerHub. The username and password for DockerHub are stored as secrets in the GitHub repository.

4. **Build and push:** This step uses the docker/build-push-action@v3 to build the Docker image and push it to DockerHub. The image is tagged as 'yurisa2/petrosa-crypto-candles-consistency-checker:v0.0.85'.

## Secrets

The workflow uses the following secrets:

- `DOCKERHUB_USERNAME`: The username for DockerHub.
- `DOCKERHUB_TOKEN`: The token for DockerHub.

## Conclusion

This workflow automates the process of building a Docker image and pushing it to DockerHub whenever there is a push to the 'main' branch. This ensures that the Docker image is always up-to-date with the latest changes in the code.