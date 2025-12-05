from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from data.interfaces.blob import (
    get_adlfs_path, 
    get_storage_options, 
    check_exists,
    get_file_client
)
import polars as pl
import logging
import uuid
from io import BytesIO

class EntityBase(ABC):
    """
    Abstract base class for entity tables with temporal versioning.
    
    All entities maintain full history through versioning:
    - Each version gets a unique Id
    - Versions of the same logical record share an InstanceId
    - Only one version per InstanceId has IsCurrent = True
    """
    
    REQUIRED_FIELDS = {
        "Id": pl.Utf8,
        "InstanceId": pl.Utf8,
        "CreatedDate": pl.Datetime("us", "UTC"),
        "IsCurrent": pl.Boolean
    }
    
    def __init__(self):
        """
        Initialize entity with storage location.
        Schema is automatically constructed from REQUIRED_FIELDS + additional_schema.
        
        Args:
            storage_path: Path to folder containing parquet files
        
        Raises:
            ValueError: If additional_schema contains reserved field names
        """
        self.storage_path = get_adlfs_path()
        self.file_path = f"{self.entity_name}/{self.entity_name}.parquet"
        self.full_url = f"{self.storage_path}{self.file_path}"
        
        # Validate that additional_schema doesn't use reserved names
        additional = self.additional_schema
        reserved_conflicts = set(additional.keys()) & set(self.REQUIRED_FIELDS.keys())
        if reserved_conflicts:
            raise ValueError(
                f"additional_schema cannot use reserved field names: {reserved_conflicts}"
            )
        
        # Construct full schema: required fields first, then additional fields
        self.schema = {**self.REQUIRED_FIELDS, **additional}
    
    @property
    @abstractmethod
    def entity_name(self) -> str:
        """Name of the entity (e.g., 'food_diary', 'food'). Used for file naming."""
        pass

    @property
    @abstractmethod
    def additional_schema(self) -> Dict[str, pl.DataType]:
        """
        Define entity-specific fields beyond the required base fields.
        
        Must NOT include: Id, InstanceId, CreatedDate, IsCurrent
        These are automatically added by the base class.
        
        Returns:
            Dictionary mapping column names to Polars dtypes
        """
        pass
    
    def load_all(self) -> pl.LazyFrame:
        """
        Load all records (including historical versions) as a LazyFrame.
        
        Returns:
            LazyFrame containing all records, or empty LazyFrame if no data exists
        """
        if not check_exists(self.file_path):
            # Return empty LazyFrame with correct schema
            logging.info(f"No data found for entity '{self.entity_name}'. Returning empty LazyFrame.")
            return pl.LazyFrame(schema=self.schema)
        
        return pl.scan_parquet(self.full_url, storage_options=get_storage_options())
    
    def load_current(self) -> pl.LazyFrame:
        """
        Load only current records (IsCurrent = True) as a LazyFrame.
        
        Returns:
            LazyFrame containing only current records
        """
        return self.load_all().filter(pl.col("IsCurrent") == True)
    
    def create(self, data: Dict[str, Any]) -> str:
        """
        Create a new entity instance.
        
        Automatically generates:
        - Id: New UUIDv7
        - InstanceId: New UUIDv7 (same as Id for initial creation)
        - CreatedDate: Current timestamp
        - IsCurrent: True
        
        Args:
            data: Dictionary of field values (excluding auto-generated fields)
        
        Returns:
            The InstanceId of the created record (as string)
        """
        instance_id = self._generate_uuid()
        record = {
            "Id": instance_id,
            "InstanceId": instance_id,
            "CreatedDate": datetime.now(timezone.utc),
            "IsCurrent": True,
            **data
        }
        
        self._write_record(record)
        return instance_id
    
    def update(self, instance_id: str, data: Dict[str, Any]) -> str:
        """
        Update an existing instance by creating a new version.
        
        Process:
        1. Marks current version as IsCurrent = False
        2. Creates new version with updated data and IsCurrent = True
        
        Automatically generates:
        - Id: New UUIDv7
        - CreatedDate: Current timestamp
        - IsCurrent: True
        
        Args:
            instance_id: The InstanceId to update
            data: Dictionary of updated field values
        
        Returns:
            The new record Id (as string)
        
        Raises:
            ValueError: If instance_id doesn't exist or has no current version
        """
        # Load all data to modify
        all_data = self.load_all().collect()
        
        # Find current version
        current = all_data.filter(
            (pl.col("InstanceId") == instance_id) & 
            (pl.col("IsCurrent") == True)
        )
        
        if current.height == 0:
            raise ValueError(
                f"No current record found for InstanceId: {instance_id}"
            )
        
        # Mark current version as not current
        updated_data = all_data.with_columns(
            pl.when(
                (pl.col("InstanceId") == instance_id) & 
                (pl.col("IsCurrent") == True)
            )
            .then(False)
            .otherwise(pl.col("IsCurrent"))
            .alias("IsCurrent")
        )
        
        # Create new version
        new_id = self._generate_uuid()
        new_record = {
            "Id": new_id,
            "InstanceId": instance_id,
            "CreatedDate": datetime.now(timezone.utc),
            "IsCurrent": True,
            **data
        }
        
        # Append new record
        new_df = pl.DataFrame([new_record], schema=self.schema)
        combined = pl.concat([updated_data, new_df])
        
        # Write back
        self._upload_to_adlfs(combined)
        
        return new_id

    def _upload_to_adlfs(self, df):
        file_client = get_file_client(self.file_path)
        buffer = BytesIO()
        df.write_parquet(buffer)
        file_client.upload_data(buffer.getvalue(), overwrite=True)
    
    def _write_record(self, record: Dict[str, Any]):
        """
        Internal method to write a single record to storage.
        
        Appends to existing data or creates new file if none exists.
        """
        new_df = pl.DataFrame([record], schema=self.schema)

        if check_exists(self.file_path):
            # Append to existing data
            existing = pl.read_parquet(self.full_url, storage_options=get_storage_options())
            combined = pl.concat([existing, new_df])
            self._upload_to_adlfs(combined)
        else:
            # Create new file
            self._upload_to_adlfs(new_df)
    
    @staticmethod
    def _generate_uuid() -> str:
        """Generate a UUIDv7 (time-ordered UUID)."""
        # Note: uuid7() requires Python 3.13+
        # For older versions, use uuid4() or a third-party library
        try:
            return str(uuid.uuid7())
        except AttributeError:
            # Fallback for Python < 3.13
            return str(uuid.uuid4())
    
    def get_instance_history(self, instance_id: str) -> pl.DataFrame:
        """
        Get all versions of a specific instance, ordered by creation date.
        
        Args:
            instance_id: The InstanceId to retrieve history for
        
        Returns:
            DataFrame containing all versions, sorted by CreatedDate
        """
        return (
            self.load_all()
            .filter(pl.col("InstanceId") == instance_id)
            .sort("CreatedDate")
            .collect()
        )