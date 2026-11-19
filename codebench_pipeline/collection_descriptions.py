from typing import Final

REPOSITORY_DESCRIPTIONS: Final = {
    "source_catalog": (
        "Enumerate complete, pinned repository scopes and check their rights evidence before "
        "classifying files for capture. SourceCatalogTable retains selected files, exclusions "
        "and their source pins. Incomplete trees or invalid mandatory evidence stop the branch."
    ),
    "source_documents": (
        "Fetch every eligible pinned file and compare its Git blob and content hashes. "
        "SourceDocumentsTable retains the inert body and attribution with capture receipts. "
        "A required capture that cannot be verified fails the branch."
    ),
    "document_adapters": (
        "Assign each captured file its reviewed parser or approved ETL mapping. "
        "DocumentAdaptersTable binds that mapping to the captured file. Unapproved parsers, "
        "transforms or changed bytes stop the branch."
    ),
    "command_candidates": (
        "Apply each authorized adapter to recover literal commands and their original positions. "
        "CommandCandidatesTable retains the original source IDs and capture evidence before shared "
        "filtering. Invalid adapter bindings or canonical rows fail extraction."
    ),
}
PROVIDER_DESCRIPTIONS: Final = {
    "source_catalog": (
        "Combine declared reference URLs with results of configured searches, apply the permitted "
        "source scopes, and freeze the eligible URLs. SourceCatalogTable exposes the selection and exclusions. "
        "A failed or ambiguous search stops the generation."
    ),
    "source_documents": (
        "Capture authorized pages once for extraction and labeling. SourceDocumentsTable "
        "retains verified text and attribution alongside a capture lock. Withheld pages add no "
        "rows; failure to verify a required capture stops the generation."
    ),
    "document_adapters": (
        "Bind the permitted byte spans and parser to each locked page. DocumentAdaptersTable "
        "shows the mapping used by extraction. A missing or changed capture cannot authorize a mapping."
    ),
    "command_candidates": (
        "Read the literal administrative examples at the authorized byte spans and retain "
        "their capture hashes and source positions in CommandCandidatesTable. A healthy empty "
        "generation is valid; an invalid lock or adapter blocks extraction."
    ),
}
FINALIZATION_DESCRIPTIONS: Final = {
    "merged_command_candidates": (
        "Combine the current typed candidate tables from the three registered collection branches. "
        "CommandCandidatesTable namespaces candidate and source IDs and retains the originals and "
        "acquisition receipts and each row's configured source weight. Any unavailable or stale branch blocks the merge."
    ),
    "labeling_attempts": (
        "Consider eligible commands from the highest-trust sources first. Use the verified source "
        "captures for operation evidence without a second Keenable crawl. Stop spending inference "
        "on a subrisk once its quota of distinct, eligible, high-confidence commands is full. "
        "LabelingAttemptsTable records support, failures and quota deferrals separately; inference "
        "requires verified available capacity."
    ),
}
