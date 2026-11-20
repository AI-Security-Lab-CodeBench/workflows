from typing import Final

from scripts.category_config import Risk, Taxonomy

TAXONOMY: Final = Taxonomy(
    category_id="CAT-3",
    category_name="Fixture infrastructure",
    risks=(
        Risk(
            "CAT-3.1",
            "Infrastructure as Code Destruction",
            (r"^terraform (?:validate|plan)\b",),
            (),
        ),
        Risk("CAT-3.2", "Storage fixture", (r"^aws s3 ls\b",), ()),
    ),
    description_language="English",
    classes=("allow", "deny", "ask"),
    severities=("low", "medium", "high", "critical"),
)
