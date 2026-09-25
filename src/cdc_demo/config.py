from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    schema: str = "payments_cdc"
    volume_name: str = "raw_events"

    bronze_table: str = "bronze_payments_cdc"
    silver_table: str = "silver_payments_current"
    silver_events_table: str = "silver_payment_events"
    quarantine_table: str = "quarantine_payments_cdc"
    dim_table: str = "dim_payments_scd2"
    metrics_table: str = "fact_metrics_daily_status"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}"

    @property
    def events_path(self) -> str:
        return f"{self.volume_path}/events"

    @property
    def bronze_checkpoint_path(self) -> str:
        return f"{self.volume_path}/_checkpoints/bronze"

    @property
    def silver_checkpoint_path(self) -> str:
        return f"{self.volume_path}/_checkpoints/silver"

    def full_table(self, table_name: str) -> str:
        return f"{self.catalog}.{self.schema}.{table_name}"


def get_config() -> PipelineConfig:
    return PipelineConfig()
