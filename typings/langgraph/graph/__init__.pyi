from collections.abc import Callable, Sequence
from typing import Self, TypedDict

from langgraph.types import Command

class _InvokeConfig(TypedDict, total=False):
    recursion_limit: int

class _CompiledStateGraph[InputT, OutputT]:
    def invoke(self, input: InputT, config: _InvokeConfig | None = None) -> OutputT: ...

class StateGraph[StateT, ContextT, InputT, OutputT]:
    def __init__(
        self,
        state_schema: type[StateT],
        context_schema: type[ContextT] | None = None,
        *,
        input_schema: type[InputT] | None = None,
        output_schema: type[OutputT] | None = None,
    ) -> None: ...
    def add_node(
        self,
        node: str,
        action: Callable[[StateT], Command[str]],
        *,
        destinations: Sequence[str],
    ) -> Self: ...
    def add_edge(self, start_key: str, end_key: str) -> Self: ...
    def compile(self) -> _CompiledStateGraph[InputT, OutputT]: ...
