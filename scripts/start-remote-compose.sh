#!/usr/bin/env bash
set -euo pipefail

cd -- "$1"
overlay=$(mktemp .codebench-local/minio.compose.XXXXXXXX)
trap 'rm -f -- "$overlay"' EXIT
printf '%s\n' "${2:?Missing MinIO Compose overlay}" > "$overlay"
mv -f -- "$overlay" .codebench-local/minio.compose.yaml
compose=(docker compose -p codebench -f compose.yaml -f .codebench-local/labeler-grounding.compose.yaml -f .codebench-local/minio.compose.yaml)
grounding_env=.codebench-runtime/grounding.env

if [[ -f $grounding_env ]]; then
    compose+=(--env-file "$grounding_env")
elif [[ -z ${CONTEXT7_API_KEY:-} || -z ${KEENABLE_API_KEY:-} ]]; then
    if ! grounding_values=$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' codebench-webserver-1 |
        awk '/^(CONTEXT7_API_KEY|KEENABLE_API_KEY)=/'); then
        printf 'Set both grounding keys on the server or provision %s before starting Compose.\n' "$grounding_env" >&2
        exit 1
    fi
    while IFS='=' read -r name value; do
        case $name in
            CONTEXT7_API_KEY) export CONTEXT7_API_KEY="${CONTEXT7_API_KEY:-$value}" ;;
            KEENABLE_API_KEY) export KEENABLE_API_KEY="${KEENABLE_API_KEY:-$value}" ;;
        esac
    done <<< "$grounding_values"
    unset grounding_values value
fi

exec "${compose[@]}" up --no-recreate --wait --wait-timeout 120 review-db minio webserver daemon
