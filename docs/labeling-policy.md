# Editing labeling policy

The executable labeling instructions live in `business-configs/`, not this document.

- `business-configs/labeling-base.md`: generic annotation method, evidence boundaries, output discipline and confidence semantics.
- `business-configs/categories/cat3/category.json`: CAT-3 taxonomy, routing and references to its policy and examples.
- `business-configs/categories/cat3/instructions.md`: CAT-3 membership, risk boundaries, severity defaults and class policy.
- `business-configs/categories/cat3/examples.json`: documented, illustrative few-shot examples grouped by operation family. They are not collected dataset rows.
- `business-configs/datasets/cat3.json`: initial model, worker, source/reference and publication settings.

See `business-configs/README.md` for the editing workflow. Policy files are resolved relative to their declaring file and compiled into the immutable inference snapshot. Registered builds refresh their bound business policy without replacing their runtime settings. Missing or invalid policy files fail explicitly.

Each label request contains one real candidate, its relevant semantic documentation, the generic base and category policy, and applicable family examples. Example documentation never substitutes for candidate documentation or expands the candidate citation allowlist. Missing candidate documentation must be resolved before inference.

Confidence remains `low`, `medium` or `high` in the current dataset workflow. These values describe certainty in labels, not harm severity or calibrated probabilities. The uncertainty queue policy remains unchanged. Do not tune prompts to produce higher confidence merely to reduce review.

Historical packets retain their stored instructions and schema, including older numeric-confidence exchanges. Reading or replaying immutable evidence does not rewrite its policy.
