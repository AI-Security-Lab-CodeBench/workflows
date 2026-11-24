#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
guide=${1:-"$root/../business-configs/bashguard/guide.json"}
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT

jq -e '
  keys == ["examples", "instructions"] and
  (.instructions | type == "string" and length > 0) and
  (.examples | type == "array" and length > 0) and
  ([.examples[].steps[].id] | length > 0 and length == (unique | length))
' "$guide" >/dev/null

(
    cd -- "$root"
    GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go build -mod=readonly -o "$tmp/bashguard" ./cmd/bashguard
)

replay() {
    local stage=$1 status=0 digest
    jq -c --arg stage "$stage" 'if $stage == "joint-finalize"
        then {version:1, rules:[.rules[]]} else {version:1, rules:[.rule]} end' "$tmp/step.json" >"$tmp/rules.json"
    jq -c '.tests[].request' "$tmp/step.json" >"$tmp/requests.jsonl"
    digest=$(sha256sum "$tmp/rules.json")
    "$tmp/bashguard" -mode classify -rules "$tmp/rules.json" \
        <"$tmp/requests.jsonl" >"$tmp/rows.jsonl" 2>"$tmp/stderr" || status=$?

    jq -en --arg stage "$stage" --arg digest "${digest%% *}" --argjson exit_code "$status" \
        --slurpfile step "$tmp/step.json" --slurpfile rows "$tmp/rows.jsonl" --rawfile stderr "$tmp/stderr" '
        def require($condition; $message): if $condition then . else error($stage + ": " + $message) end;
        def clean_negative: .status == "abstain" and .reasons[0] == "no_proven_rule"
            and .rule_ids == [] and (has("labels") | not);
        def projection: {status,basis,rule_ids,reasons} + (if has("labels") then {labels} else {} end);
        $step[0] as $step
        | $step.expected_tool_response as $tool
        | INDEX($step.tests[]; .request.id) as $tests
        | INDEX($rows[]; .id) as $by_id
        | require($exit_code == $tool.exit_code; "exit \($exit_code), expected \($tool.exit_code): \($stderr)")
        | require(all($tool.stderr_contains[]; . as $part | $stderr | contains($part)); "unexpected stderr: \($stderr)")
        | require(($by_id | keys) == ($tests | keys) and ($rows | length) == ($step.tests | length);
            "missing, extra or duplicate JSONL records: \($rows | map(.id))")
        | require(all($tool.mismatches | keys[]; . as $id | $tests[$id] and $tests[$id].expected != $tool.mismatches[$id]);
            "a declared mismatch does not distinguish desired and observed")
        | require(all($step.tests[]; .expected | .status == "matched" or clean_negative);
            "fixture targets must be a match or a clean negative")
        | [
            $step.tests[] as $test
            | $by_id[$test.request.id] as $row
            | ($tool.mismatches[$test.request.id] // $test.expected) as $expected
            | require(($row | has("error") | not) and $row.result.bundle_sha256 == $digest;
                "invalid or unpinned row \($test.request.id): \($row)")
            | ($row.result | projection) as $actual
            | require($actual == $expected; "\($test.request.id) expected \($expected) but got \($actual)")
            | require(
                if $actual.status == "matched"
                then $row.result.rationales == [($step.rules // [$step.rule])[] | select(
                    .id as $id | $actual.rule_ids | index($id) != null) | .rationale]
                    and ($row.result.sources | length) > 0
                else $row.result.rationales == [] and $row.result.sources == [] end;
                "unexpected match provenance on \($test.request.id)")
            | $actual
          ] as $actuals
        | {stage:$stage, kind:$tool.kind, exit_code:$exit_code, requests:($rows | length),
           matches:([$actuals[] | select(.status == "matched")] | length),
           clean_negatives:([$actuals[] | select(clean_negative)] | length),
           runtime_errors:([$actuals[] | select(any(.reasons[]; startswith("rule_evaluation_error:")))] | length),
           observed_mismatches:($tool.mismatches | length), bundle_sha256:$digest}
        | if $stderr == "" then . else . + {stderr:$stderr} end
    '
}

jq -c '.examples[].steps[]' "$guide" >"$tmp/steps.jsonl"
while IFS= read -r step; do
    printf '%s\n' "$step" >"$tmp/step.json"
    replay "$(jq -r '.id' "$tmp/step.json")"
done <"$tmp/steps.jsonl"

jq -ce '
    [.examples[].steps[]] as $steps
    | [.examples[] | .decision // empty] as [$decision]
    | [$decision.keep_rule_ids[] as $id
        | $steps[] | select(.rule.id == $id and .expected_tool_response.kind == "passed")] as $keep
    | [$decision.drop_rule_ids[] as $id | $steps[] | select(.rule.id == $id)] as $drop
    | [$decision.add_step_ids[] as $id | $steps[] | select(.id == $id)] as $add
    | if ($keep | map(.rule.id)) != $decision.keep_rule_ids
         or ($drop | map(.rule.id)) != $decision.drop_rule_ids
         or ($add | map(.id)) != $decision.add_step_ids
         or ($add | length) == 0
         or ($decision.justification | length) == 0
         or ([$keep[], $drop[], $add[].rule | .rule.id? // .id] | length != (unique | length))
         or any($add[]; .expected_tool_response.kind != "passed")
      then error("invalid guide add/drop diff") else . end
    | {rules:[$keep[], $add[] | .rule], tests:[$keep[], $add[] | .tests[]],
       expected_tool_response:{kind:"joint_passed",exit_code:0,stderr_contains:[],mismatches:{}}}
' "$guide" >"$tmp/step.json"
replay joint-finalize
digest=$(sha256sum "$guide")
go_version=$(GOTOOLCHAIN=local go version)
jq -n --arg digest "${digest%% *}" --arg go_version "$go_version" \
    '{status:"PASS",guide_sha256:$digest,go_version:$go_version}'
