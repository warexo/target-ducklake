"""ducklake target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_ducklake.sinks import (
    ducklakeSink,
)


class Targetducklake(Target):
    """Sample target for ducklake."""

    name = "target-ducklake"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_url",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            title="Catalog URL",
            description="URL connection string to your catalog database",
        ),
        th.Property(
            "data_path",
            th.StringType(nullable=False),
            title="Data Path",
            description="GCS, S3, or local folder path for data storage",
        ),
        th.Property(
            "storage_type",
            th.StringType(
                allowed_values=["GCS", "S3", "local"],
                nullable=False,
            ),
            default="local",
            title="Storage Type",
            description="Type of storage: GCS, S3, or local",
        ),
        th.Property(
            "public_key",
            th.StringType(nullable=True),
            title="Public Key",
            description="Public key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "secret_key",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Secret Key",
            description="Secret key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "region",
            th.StringType(nullable=True),
            title="Region",
            description="AWS region for S3 storage type (required when using S3 with explicit credentials)",
        ),
        th.Property(
            "default_target_schema",
            th.StringType(nullable=True),
            title="Default Target Schema Name",
            description=(
                "Default database schema where data should be written. "
                "If not provided schema will attempt to be derived from the stream name "
                "(e.g. database taps provide schema name in the stream name)."
            ),
        ),
        th.Property(
            "target_schema_prefix",
            th.StringType(nullable=True),
            title="Target Schema Prefix",
            description=(
                "Prefix to add to the target schema name. "
                "If not provided, no prefix will be added."
                "May be useful if target schema name is inferred from the stream name "
                "and you want to add a prefix to the schema name."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType(),
            default=False,
            title="Add Singer Record Metadata",
            description=(
                "When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables: "
            ),
        ),
        th.Property(
            "flatten_max_level",
            th.IntegerType(),
            default=0,
            title="Flattening Max Level",
            description="Maximum depth for flattening nested fields. Set to 0 to disable flattening.",
        ),
        th.Property(
            "temp_file_dir",
            th.StringType(),
            default="temp_files/",
            title="Temporary File Directory",
            description="Directory path for storing temporary parquet files",
        ),
        th.Property(
            "max_batch_size",
            th.IntegerType(),
            default=10000,
            title="Max Batch Size",
            description="Maximum number of records to process in a single batch",
        ),
        th.Property(
            "partition_fields",
            th.ObjectType(
                additional_properties=th.ArrayType(
                    th.ObjectType(
                        th.Property("column_name", th.StringType(nullable=False)),
                        th.Property(
                            "type",
                            th.StringType(
                                allowed_values=["timestamp", "identifier"],
                                nullable=False,
                            ),
                        ),
                        th.Property(
                            "granularity",
                            th.ArrayType(
                                th.StringType(
                                    allowed_values=["year", "month", "day", "hour"]
                                )
                            ),
                            nullable=True,
                        ),
                    )
                )
            ),
            nullable=True,
            title="Partition Fields",
            description=(
                "Object mapping stream names to arrays of partition column definitions. "
                "Each stream key maps directly to an array of column definitions."
            ),
        ),
        th.Property(
            "auto_cast_timestamps",
            th.BooleanType(),
            default=False,
            title="Auto Cast Timestamps",
            description=(
                "When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake."
            ),
        ),
    ).to_dict()

    default_sink_class = ducklakeSink


if __name__ == "__main__":
    Targetducklake.cli()
