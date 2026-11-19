# Editing labeling policy

The executable labeling instructions live in `business-configs/`, not this document.

- `business-configs/labeling/instructions.md`: generic annotation method, evidence boundaries, output discipline and confidence semantics.
- `business-configs/labeling/policy.json`: shared class and severity definitions and description language.
- `business-configs/categories/cat3/category.json`: CAT-3 taxonomy, routing and optional restrictions on allowed severities.
- `business-configs/categories/cat3/instructions.md`: CAT-3 membership, risk boundaries, severity defaults and class policy.
- `business-configs/categories/cat3/examples.json`: documented, illustrative few-shot examples grouped by operation family. They are not collected dataset rows.
- `business-configs/datasets/cat3.json`: initial model, worker, source/reference and publication settings.

See `business-configs/README.md` for the editing workflow. Versioned dataset configs resolve policy files relative to the declaring file at registration. Edit and register the bundle to change the effective policy. Inspect `labeling_attempts` with `dataset_cli inspect-config` to see the model settings and compiled policy before registration. For batched labeling, `finalization.annotation.batch.instructions` adds operator guidance to the model request; changing that guidance invalidates cached judgments. The tool protocol, evidence checks and output schema still apply.

Each label request contains one real candidate, its relevant semantic documentation, the generic base and category policy, and applicable family examples. Example documentation never substitutes for candidate documentation or expands the candidate citation allowlist. Missing candidate documentation must be resolved before inference.

Confidence remains `low`, `medium` or `high` in the current dataset workflow. These values describe certainty in labels, not harm severity or calibrated probabilities. The uncertainty queue policy remains unchanged. Do not tune prompts to produce higher confidence merely to reduce review.

Historical packets retain their stored instructions and schema, including older numeric-confidence exchanges. Reading or replaying immutable evidence does not rewrite its policy.
