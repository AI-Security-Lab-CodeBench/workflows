#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
    printf 'Copy .env.example to .env and configure remote-up.\n' >&2
    exit 1
fi

while IFS= read -r line || [[ -n $line ]]; do
    case $line in
        REMOTE_SSH=*) remote_ssh=${line#*=} ;;
        REMOTE_SSH_PORT=*) remote_ssh_port=${line#*=} ;;
        REMOTE_COMPOSE_DIR=*) remote_compose_dir=${line#*=} ;;
    esac
done < .env

: "${remote_compose_dir:?Set REMOTE_COMPOSE_DIR in .env}"
if [[ -z ${remote_ssh:-} ]]; then
    bash scripts/start-remote-compose.sh "$remote_compose_dir"
else
    ssh_options=(-p "${remote_ssh_port:-22}" -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=5)
    socket="$HOME/.ssh/codebench-3041-%C"
    { printf 'set -- %q\n' "$remote_compose_dir"; cat scripts/start-remote-compose.sh; } |
        ssh "${ssh_options[@]}" -- "$remote_ssh" 'bash -s'
    if ! ssh "${ssh_options[@]}" -S "$socket" -O check -- "$remote_ssh" >/dev/null 2>&1; then
        ssh "${ssh_options[@]}" -S "$socket" -MfN \
            -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
            -L 127.0.0.1:3041:127.0.0.1:3041 -- "$remote_ssh"
    fi
fi

curl --noproxy '*' --fail --silent --show-error --location --max-time 10 \
    --output /dev/null --write-out 'http://localhost:3041/ (HTTP %{http_code})\n' \
    http://127.0.0.1:3041/
