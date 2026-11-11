from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition, Config, Output, asset

from codebench_pipeline.acquisition import acquire
from codebench_pipeline.discovery import discover
from codebench_pipeline.storage import Registry


class SearchConfig(Config):
    request_file: str


class AcquisitionConfig(Config):
    dataset_id: str
    revision: str
    semantics_file: str
    model: str = "chatgpt-subscription/gpt-6-luna"


def discovery_assets(root: Path) -> list[AssetsDefinition]:
    @asset(group_name="discovery", description="Keenable searches and inspected-source agent selection.")
    def source_discovery(context: AssetExecutionContext, config: SearchConfig) -> Output[str]:
        destination = root / "discovery" / context.run_id
        result = discover(Path(config.request_file), destination)
        if result.failures and not result.inspected:
            raise RuntimeError(f"Discovery failed; inspect {destination}/discovery.json")
        return Output(
            str(destination / "discovery.json"),
            metadata={
                "sources": len(result.sources),
                "inspected_pages": len(result.inspected),
                "failures": len(result.failures),
                "artifact": str(destination),
            },
        )

    @asset(
        group_name="discovery", description="Acquire permitted sources and register a new dataset partition."
    )
    def source_acquisition(
        context: AssetExecutionContext,
        config: AcquisitionConfig,
        source_discovery: str,
    ) -> Output[str]:
        destination = root / "acquisitions" / context.run_id
        spec = acquire(
            Path(source_discovery),
            destination,
            config.dataset_id,
            config.revision,
            Path(config.semantics_file),
            config.model,
        )
        key = Registry(root).register(spec)
        context.instance.add_dynamic_partitions("datasets", [key])
        return Output(key, metadata={"dataset": key, "manifest": str(spec.documents)})

    return [source_discovery, source_acquisition]
