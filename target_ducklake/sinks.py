"""ducklake target sink class, which handles writing streams."""

from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import shutil
from typing import Sequence

# from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import BatchSink

from target_ducklake.flatten import flatten_schema, flatten_record
from singer_sdk import Target
import pyarrow.parquet as pq

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.parquet_utils import (
    flatten_schema_to_pyarrow_schema,
    concat_tables,
)


class ducklakeSink(BatchSink):
    """ducklake target sink class."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.temp_file_dir = self.config.get("temp_file_dir", "temp_files/")
        self.files_saved = 0

        # Use the connector for database operations
        self.connector = DuckLakeConnector(dict(self.config))

        # Create pyarrow and Ducklake schemas
        self.flatten_max_level = self.config.get("flatten_max_level", 0)
        self.auto_cast_timestamps = self.config.get("auto_cast_timestamps", False)
        self.flatten_schema = flatten_schema(
            self.schema, max_level=self.flatten_max_level, auto_cast_timestamps=self.auto_cast_timestamps
        )
        self.pyarrow_schema = flatten_schema_to_pyarrow_schema(self.flatten_schema)
        self.ducklake_schema = self.connector.json_to_ducklake_schema(
            self.flatten_schema
        )

        # # Log the schemas for debugging
        self.logger.info(f"Original schema for stream '{stream_name}': {self.schema}")
        self.logger.info(
            f"Flattened schema for stream '{stream_name}': {self.flatten_schema}"
        )
        self.logger.info(
            f"PyArrow schema for stream '{stream_name}': {self.pyarrow_schema}"
        )
        self.logger.info(
            f"DuckLake schema for stream '{stream_name}': {self.ducklake_schema}"
        )

        # Initialize partition fields (if any)
        partition_fields = self.config.get("partition_fields")
        self.partition_fields = (
            partition_fields.get(self.stream_name) if partition_fields else None
        )

        # Log for type of data load (append or merge)
        if self.key_properties:
            self.logger.info(
                f"Key properties found for {self.stream_name}, {self.key_properties}, merging data"
            )
        else:
            self.logger.info(
                f"No key properties found for {self.stream_name}, appending data"
            )

    @property
    def target_schema(self) -> str:
        """Get the target schema."""
        if self.config.get("default_target_schema"):
            # if default target schema is provided, use it
            self.logger.info(
                f"Using provided default target schema {self.config.get('default_target_schema')}"
            )
            return self.config.get("default_target_schema")  # type: ignore
        else:
            # if no default target schema is provided, try to derive it from the stream name
            stream_name_dict = stream_name_to_dict(self.stream_name)
            if stream_name_dict.get("schema_name"):
                if self.config.get("target_schema_prefix"):
                    target_schema = f"{self.config.get('target_schema_prefix')}_{stream_name_dict.get('schema_name')}"
                else:
                    target_schema = stream_name_dict.get("schema_name")
                self.logger.info(
                    f"Using derived target schema {target_schema} from stream name {self.stream_name}"
                )
                return target_schema
            else:
                raise ValueError(
                    f"No schema name found for stream {self.stream_name} and no default target schema provided"
                )

    @property
    def target_table(self) -> str:
        """Get the target table."""
        stream_name_dict = stream_name_to_dict(self.stream_name)
        return stream_name_dict.get("table_name")

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        """
        Process incoming record and convert Decimal objects to float for PyArrow compatibility
        when writing to parquet files.
        """
        # Convert any Decimal objects to float so that we can write to parquet files
        for key, value in record.items():
            if isinstance(value, Decimal):
                record[key] = float(value)
                self.logger.debug(f"Converted Decimal field '{key}' from {value} to {record[key]}")
        
        return record

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record and flatten if necessary."""
        record_flatten = flatten_record(
            record,
            flatten_schema=self.flatten_schema,
            max_level=self.flatten_max_level,
        )
        super().process_record(record_flatten, context)

    def write_temp_file(self, context: dict) -> str:
        """Write the current batch to a temporary parquet file."""
        self.logger.info(
            f'Writing batch for {self.stream_name} with {len(context["records"])} records to parquet file.'
        )
        pyarrow_df = None
        pyarrow_df = concat_tables(
            context.get("records", []), pyarrow_df, self.pyarrow_schema  # type: ignore
        )
        self.logger.info(
            f"Batch pyarrow table has ({len(pyarrow_df)} rows)"  # type: ignore
        )
        del context["records"]

        # Ensure temp directory exists
        os.makedirs(self.temp_file_dir, exist_ok=True)

        # Create a more descriptive filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"{self.stream_name}_{timestamp}_{self.files_saved:05d}.parquet"
        full_temp_file_path = os.path.join(self.temp_file_dir, file_name)

        try:
            self.logger.info(f"Writing batch to temp file: {full_temp_file_path}")
            pq.write_table(pyarrow_df, full_temp_file_path)
            self.files_saved += 1
            row_count = len(pyarrow_df) if pyarrow_df is not None else 0
            self.logger.info(f"Successfully wrote {row_count} rows to {file_name}")
            return full_temp_file_path
        except Exception as e:
            self.logger.error(
                f"Failed to write parquet file {full_temp_file_path}: {e}"
            )
            raise

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""

        # create the target schema if it doesn't exist
        self.connector.prepare_target_schema(self.target_schema)

        # first write each batch to a parquet file
        temp_file_path = self.write_temp_file(context)
        self.logger.info(f"Temp file path: {temp_file_path}")

        # prepare the table
        table_columns = self.connector.prepare_table(
            self.target_schema,
            self.target_table,
            self.ducklake_schema,
            self.partition_fields,
        )
        file_columns = [col["name"] for col in self.ducklake_schema]

        # If no key properties, we simply append the data
        if not self.key_properties:
            self.connector.insert_into_table(
                temp_file_path,
                self.target_schema,
                self.target_table,
                file_columns,
                table_columns,
            )

        # If key properties, we upsert the data
        elif self.key_properties:
            self.connector.merge_into_table(
                temp_file_path,
                self.target_schema,
                self.target_table,
                file_columns,
                table_columns,
                self.key_properties,
            )

        # delete file after insert
        os.remove(temp_file_path)

    def __del__(self) -> None:
        """Cleanup when sink is destroyed."""
        if hasattr(self, "connector"):
            self.connector.close()
        # delete the temp file
        if hasattr(self, "temp_file_dir"):
            self.logger.info(f"Cleaning up temp file directory {self.temp_file_dir}")
            if os.path.exists(self.temp_file_dir):
                shutil.rmtree(self.temp_file_dir)


def stream_name_to_dict(stream_name, separator="-"):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = "_".join(s[2:])

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }
