from typing import Final

DECLARED_COLLECTION_DESCRIPTIONS: Final = {
    "enumerate_corpus_files": (
        "Read the pinned corpus trees and match their files to the declared scopes after checking "
        "rights. The SourceCatalogTable records admitted and excluded paths. An incomplete tree or "
        "missing rights evidence stops the branch. Reuse this index when retrying capture."
    ),
    "capture_corpus_files": (
        "Fetch the admitted files from the stored corpus index and check their Git blob and content "
        "hashes. SourceDocumentsTable retains verified text and attribution. Failed mandatory captures "
        "stop extraction; verified blobs can be reused on retry."
    ),
    "command_candidates": (
        "Resolve each captured corpus file's approved ETL against the deployment allowlist, then run it "
        "against verified bytes. CommandCandidatesTable retains inert commands, input positions and "
        "transformations. A changed capture or unapproved ETL fails extraction."
    ),
    "documentation/capture_documentation": (
        "Verify the pinned documentation trees, apply the declared scope and rights checks, and capture "
        "every admitted file in one asset. SourceDocumentsTable retains verified text and attribution; "
        "its evidence includes excluded paths. Missing mandatory files or rights block the branch."
    ),
    "documentation/command_candidates": (
        "Use the approved documentation parser on the verified captures to extract literal examples and "
        "nearby context. CommandCandidatesTable links each example to its original file and position. "
        "A changed capture or unapproved parser fails extraction."
    ),
    "keenable/discover_use_cases": (
        "Record the leads from bounded, scoped searches and declared reference URLs. SourceCatalogTable "
        "exposes selections and exclusions before any selected page is captured. Transient search failures "
        "back off and retry within the call budget; saved successes are reused. A permanent failure, "
        "ambiguous dispatch or exhausted retry allowance stops discovery."
    ),
    "keenable/capture_origin_pages": (
        "Verify usage rights and fetch the selected pages directly from their origins. "
        "SourceDocumentsTable stores the verified text and the capture lock. Withheld pages add no "
        "rows; an uncertain or failed mandatory request blocks the generation."
    ),
    "keenable/command_candidates": (
        "Resolve the permitted parser and byte spans against the capture lock, then read the literal "
        "administrative examples. CommandCandidatesTable retains exact positions and capture evidence. "
        "A healthy empty generation is valid; an invalid lock or span binding blocks extraction."
    ),
}

FINALIZATION_DESCRIPTIONS: Final = {
    "merged_command_candidates": (
        "Combine the current typed candidate tables from the three registered collection branches. "
        "Use each repository candidate's receipt to identify its origin and apply the merge configuration's "
        "origin override or branch default. Keenable uses its branch weight. CommandCandidatesTable prefixes "
        "candidate and source IDs with the branch and retains the original IDs and acquisition receipts. "
        "Any unavailable or stale configured branch blocks the merge."
    ),
    "labeling_attempts": (
        "Consider eligible commands from the highest-trust sources first. Use the verified source "
        "captures for operation evidence without a second Keenable crawl. Stop spending inference "
        "on a subrisk once its quota of distinct, eligible, high-confidence commands is full. "
        "LabelingAttemptsTable records support, failures and quota deferrals separately; inference "
        "requires verified available capacity."
    ),
}
