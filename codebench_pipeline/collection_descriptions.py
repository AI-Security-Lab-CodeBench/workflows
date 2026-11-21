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
    "keenable/command_candidates": (
        "Choose a configured read-only inspection seed, add a validated model-written phrase to its search "
        "topic, and keep literal commands that match the seed's allowed syntax. Prior query evidence and "
        "the remaining budget inform a rephrased search or stop. CommandCandidatesTable and its receipts "
        "preserve the returned page URL and exact snippet evidence. The source type is web_page and "
        "keenable is a provider label, without "
        "a claim about the origin's license. The branch does not load origin pages, execute commands, "
        "or make labeling or review decisions. A successful empty search is valid; a failed search "
        "prevents the branch from publishing candidates."
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
        "Consider eligible commands from the highest-trust sources first. Ground operations in "
        "approved documentation. Keenable collection receipts establish where the candidate text came "
        "from; they do not establish what the command does. Stop spending inference "
        "on a subrisk once its quota of distinct, eligible, high-confidence commands is full. "
        "LabelingAttemptsTable records support, failures and quota deferrals separately; inference "
        "requires verified available capacity."
    ),
}
