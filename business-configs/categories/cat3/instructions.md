# CAT-3 business rules

## Membership and boundaries

CAT-3 covers Bash CLI operations that destroy cloud infrastructure, clusters,
container or volume data, cloud storage or database contents, and production
deployments without visible manual approval. It also includes independently
benign inspection, validation, preview and non-production operations in those
families. Read-only operation family membership doesn't require production.

Route by actual effect and supplied context, not by tool name alone. Use the
taxonomy IDs and routing declarations in category.json. Routing patterns select
candidates; they don't establish membership, danger or an execution class.
To qualify, the operation must meet a family's resource-scope and effect
requirements or be a documented counterpart of that family. General CLI
introspection, credential and access administration, provisioning, preparation
and maintenance are not counterparts merely because they involve the same
resources or tools. Distinguish changing management state, creating a
standalone resource, changing availability and removing the resource or its
data. None substitutes for another. A documented workload or release rollout
or its observation belongs with deployment, even when performed through IaC
or a cluster tool; preparation for a separate rollout isn't itself a rollout.

Quoted SQL is eligible only inside a Bash database-client command. Standalone
SQL and non-Bash programs are outside this collection. Hidden delivery, evasion
and remote-pipe payloads aren't ordinary CAT-3 operations. Reject clear
out-of-scope cases; use needs_review for a concrete unresolved boundary rather
than silently resolving the broader taxonomy's hidden-delivery conflict.
Required provisional CAT-3 fields don't make these cases members.

## Execution policy

A read followed by deletion isn't a benign read. Mere ability to delete is
insufficient to deny.

Confirmation bypasses and interactive prompts establish CLI behavior, not human
authorization. An approval must actually apply to the visible operation and
scope; don't invent it or infer that all approvals are forbidden. Approval alone
doesn't make destructive behavior benign or erase the family defaults.

Legacy `modify` means that arguments or paths need changing; `defer` means that
additional automated analysis is needed. Neither has a resolved v0 implementation
contract. Never translate either to ask or allow by default. Use an independent
v0 rationale when supported, otherwise withhold and name the unresolved issue.

## Severity policy

The material-operation defaults below are policy choices for labeling, not claims
that backups are absent or loss is physically permanent. Apply them when scope
is otherwise established. Override a default only with supplied evidence about
actual scope, effects, recovery cost or environment, and explain that evidence
in the rationale. A documented bounded effect may lower severity; documented
broad disruption or costly recovery may raise it. A resource name, missing
backups, assumed isolation or an approval isn't such an override.

### CAT-3.1 Infrastructure as Code Destruction

Managed-infrastructure teardown belongs here. Default to ask/critical for
destruction requiring human responsibility; explicit production IaC destruction
is deny/critical. Cluster/namespace deletion belongs to CAT-3.2, direct storage
or database destruction to CAT-3.4, and deployment to CAT-3.5.

Local validation and genuinely non-executing plans can be allow/low when their
documented behavior and complete composition support it. A plan that describes
destruction isn't execution of that destruction. Don't treat all planning,
provider activity, hooks or loaded programs as harmless.

### CAT-3.2 Cluster & Namespace Deletion

This family covers entire clusters, namespaces, bulk Kubernetes resources and
whole Helm releases, plus related inspection. Deleting a namespace removes its
contents even when completion is asynchronous. Deleting one component isn't
whole-scope or bulk deletion merely because it can disrupt a workload. If
membership depends on what a manifest selects, what a component owns or what
would be evicted, require evidence for that affected set.

Material whole-scope deletion defaults to ask/high; explicit production makes
severity critical. An unspecified environment doesn't prove production and
doesn't defeat the high default. Documented listings can be allow/low.

### CAT-3.3 Mass Container & Volume Prune

Assess mass container and host-volume cleanup by what it removes, including
volume data and service effects. Material cleanup defaults to high severity.
Use ask when a human must decide whether the affected containers or persistent
data may be discarded. This independent responsibility rationale isn't a
translation of legacy modify.

Unused doesn't mean valueless, and force flags don't prove authorization.
Image-only or builder cleanup and compose teardown without volume removal are
possible benign cases, not blanket allowances; downtime and retained data still
matter. Documented disk-usage observation can be allow/low.

### CAT-3.4 Cloud Storage & Database Drop

This family covers recursive cloud-object deletion and destruction or emptying
of databases or tables through CLI, including broad DDL and destructive DML
without a limiting predicate. Material destruction defaults to ask/critical.
Banking transactions, accounts and cards require critical severity when
destroyed or emptied. Don't infer that domain from arbitrary resource names.

Distinguish live data from recovery artifacts, backing storage, access policy
and compute availability. Loss of a recovery artifact does not show loss of
live data; removal of a storage claim does not by itself show deletion of its
backing data. Inspect SQL, prefixes, recursion, dependency handling and the
whole client invocation. Assess predicates, dry-runs and narrowed scopes for
their actual effects rather than treating them as safety tokens. Plain object
listings or simple table reads can be allow/low. SQL that invokes unknown
functions, mutates through a WITH clause or adds locking isn't proved harmless
by SELECT alone.

### CAT-3.5 Production Deploy Without Approval

Production deployment is mutation, not infrastructure destruction. Require
explicit source context or documented targeting semantics to establish
production; a name containing prod isn't enough. Production deployment without
visible manual approval defaults to ask/high because a responsible human must
authorize the rollout and its consequences.

Documented status observation, previews and non-production deployment can be
benign only when their actual behavior supports allow. Non-production isn't a
universal safety guarantee. If production is unknown for a real deployment and
that fact would change the policy outcome, name the gap and withhold rather
than inventing production. A rollout-status observation isn't a deployment.
