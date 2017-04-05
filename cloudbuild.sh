#!/bin/bash


if [ -z "$PROJECT_ID" ]; then
    echo "Need to set PROJECT_ID"
    exit 1
fi

if [ -z "$BUILD_VERSION" ]; then
    echo "Need to set BUILD_VERSION"
    exit 1
fi

set -ex

if command -v envsubst >/dev/null 2>&1; then
  envsubst < cloudbuild.yml.in > cloudbuild.yml
else
  echo "envsubst we might be in OSX"
  /usr/local/Cellar/gettext/0.19.8.1/bin/envsubst < cloudbuild.yml.in > cloudbuild.yml
fi

gcloud container builds submit --config=cloudbuild.yml .
