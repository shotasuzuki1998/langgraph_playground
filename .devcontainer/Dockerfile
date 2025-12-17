FROM mcr.microsoft.com/devcontainers/base:ubuntu

RUN apt-get update && apt-get install -y \
    default-mysql-client \
    git-secrets \
    git-lfs \
    gh \
    curl \
    && rm -rf /var/lib/apt/lists/*