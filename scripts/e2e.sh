#!/usr/bin/env bash
set -eu

work=$(mktemp -d /tmp/codebench-e2e.XXXXXXXX)
stage=setup
server=
finish() {
    rc=$?
    trap - EXIT
    if [[ -n $server ]]; then
        kill -INT "$server" 2>/dev/null || :
        wait "$server" || :
    fi
    if (( rc == 0 )); then
        rm -rf "$work"
    else
        printf 'E2E failure during %s; logs: %s\n' "$stage" "$work" >&2
    fi
    printf 'E2E completed in %ss (exit %s)\n' "$SECONDS" "$rc"
    exit "$rc"
}
trap finish EXIT
trap 'exit 124' TERM
export HOME="$work/home" DAGSTER_HOME="$work/dagster" DAGSTER_DISABLE_TELEMETRY=1
mkdir "$HOME" "$DAGSTER_HOME"
initdb -D "$work/db" -A trust -U e2e -E UTF8 --locale=C >"$work/init.log"
postgres -D "$work/db" -k "$work" -h '' >"$work/postgres.log" 2>&1 &
server=$!
until pg_isready -q -h "$work" -U e2e -d postgres; do
    kill -0 "$server"
    sleep 0.1
done
export CODEBENCH_REVIEW_DATABASE_URL="postgresql://e2e@/postgres?host=$work"
printf 'E2E isolated database ready at %ss\n' "$SECONDS"
stage='three-branch preparation, labeling, review and guarded release'
printf 'E2E starting %s\n' "$stage"
export PYTHONPATH="tests${PYTHONPATH:+:$PYTHONPATH}"
python3 -B -m unittest -v test_guarded_publication.GuardedPublicationTests.test_exact_release_requires_attestation_and_hold_clearance_and_rechecks_cached_output
printf 'E2E release verified at %ss; stopping isolated database\n' "$SECONDS"
stage=cleanup
