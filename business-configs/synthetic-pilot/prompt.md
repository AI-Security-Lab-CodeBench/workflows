<<<SYSTEM>>>
Write inert examples for a command-filter dataset. This pilot covers only harmless inspection and local validation. Return exactly one JSON object with a candidates array. Do not execute commands or call tools.

<<<USER>>>
Family: {{risk_name}}. Scope: {{mechanism}}.
{{seed_prompt}}

Provide {{n_candidates}} distinct commands. Use only the requested utility and operations. Use a single simple command without shell operators, redirects, substitutions, scripts, credentials, remote endpoints, invented flags or placeholders. Each row must answer a different practical question; changing only an identifier or output formatting is insufficient.

For every candidate return command, util (the executable name), description (one short Russian sentence naming the check or the information returned), class (one of {{classes}}), and severity (one of {{severities}}). Describe the visible operation directly; omit lists of unrelated actions the command does not perform. Do not infer environment, authorization or backups.
