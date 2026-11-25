FROM ghcr.io/mlflow/mlflow:v3.18.0@sha256:257204c77980b0f3450a26eb6089e487603fcdd4509529eadaf88f90669f5c13
RUN install -d -m 0750 -o 65532 -g 65532 /var/lib/mlflow/state /var/lib/mlflow/state/artifacts
ENV HOME=/tmp
WORKDIR /tmp
USER 65532:65532
