import pandera.pandas as pa
from pandera.typing import Series


class SplitRowsTable(pa.DataFrameModel):
    """An ordered, lossless table of the nine pinned CSV columns."""

    command: Series[str]
    description: Series[str]
    util: Series[str]
    severity: Series[str]
    category: Series[str]
    risk: Series[str]
    source: Series[str]
    seed: Series[str]
    class_: Series[str] = pa.Field(alias="class")

    class Config(pa.DataFrameModel.Config):
        strict = True
        ordered = True
        coerce = False
        unique_column_names = True
