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

if [[ -z ${3:-} ]]; then
    exec "${compose[@]}" up --no-recreate --wait --wait-timeout 120 review-db minio webserver daemon
fi

revision=$3
files=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}' codebench-webserver-1)
IFS=, read -ra overrides <<< "$files"
for file in "${overrides[@]}"; do
    case "$(realpath "$file")" in
        "$PWD/compose.yaml"|"$PWD/.codebench-local/labeler-grounding.compose.yaml"|"$PWD/.codebench-local/minio.compose.yaml") ;;
        *) test -f "$file"; compose+=(-f "$file") ;;
    esac
done
checkout_for() {
    docker inspect --format '{{range .Mounts}}{{if and (eq .Destination $.Config.WorkingDir) (not .RW)}}{{.Source}}{{end}}{{end}}' "codebench-$1-1"
}
checkout=$(checkout_for webserver)
[[ -n $checkout && $checkout == "$(checkout_for daemon)" ]]
test -z "$(git -C "$checkout" status --porcelain)"
"${compose[@]}" exec -T webserver dagster sensor stop -w "$PWD/.codebench-runtime/compose/workspace.yaml" dataset_build_requests
git -C "$checkout" fetch origin master
test "$(git -C "$checkout" rev-parse FETCH_HEAD)" = "$revision"
git -C "$checkout" merge --ff-only "$revision"
test "$(git -C "$checkout" rev-parse HEAD)" = "$revision"
"${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 120 webserver daemon
for service in webserver daemon; do
    [[ $(checkout_for "$service") == "$checkout" ]]
    "${compose[@]}" exec -T "$service" sh -eu -c 'test -r "$AWS_SHARED_CREDENTIALS_FILE"; test "$(git -c safe.directory="$PWD" rev-parse HEAD)" = "$1"' sh "$revision"
done
"${compose[@]}" exec -T webserver sh -eu -c '
    python -m codebench_pipeline.dataset_cli register --config business-configs/datasets/cat3.json --home "$CODEBENCH_HOME"
    exec python -m codebench_pipeline.dataset_cli build --dataset cloud_infrastructure_commands_grounded_v1 --home "$CODEBENCH_HOME" --force --prepare-only
'
