"""Validation utilities for configuration and data models.

This module provides Pydantic models and JSON schema validation
to ensure type safety and correct configuration throughout the batch system.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore

try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False


class BatchConfig(BaseModel if PYDANTIC_AVAILABLE else object):
    """Pydantic model for batch configuration.

    Provides runtime validation of batch_config.json structure.
    """
    if PYDANTIC_AVAILABLE:
        enabled_tables: List[str] = Field(
            ...,
            min_length=1,
            description="List of DynamoDB table names to process"
        )
        default_system_prompt: Optional[str] = Field(
            None,
            min_length=1,
            description="Default system prompt for OpenAI completions"
        )
        table_prompts: Optional[Dict[str, str]] = Field(
            default_factory=dict,
            description="Table-specific system prompts"
        )

        @field_validator('enabled_tables')
        @classmethod
        def validate_table_names(cls, v: List[str]) -> List[str]:
            """Validate that table names are non-empty and unique."""
            if not v:
                raise ValueError("enabled_tables cannot be empty")
            for table in v:
                if not table.strip():
                    raise ValueError("Table names must be non-empty strings")
            if len(v) != len(set(v)):
                raise ValueError("enabled_tables must contain unique values")
            return v

        @field_validator('table_prompts')
        @classmethod
        def validate_prompts(cls, v: Optional[Dict[str, str]]) -> Dict[str, str]:
            """Validate that all prompts are non-empty strings."""
            if v is None:
                return {}
            for table_name, prompt in v.items():
                if not isinstance(prompt, str) or not prompt.strip():
                    raise ValueError(f"Prompt for table '{table_name}' must be a non-empty string")
            return v


def validate_config_with_schema(config: Dict[str, Any], schema_path: str) -> None:
    """Validate configuration against JSON schema.

    Args:
        config: The configuration dictionary to validate
        schema_path: Path to the JSON schema file

    Raises:
        jsonschema.ValidationError: If validation fails
        FileNotFoundError: If schema file doesn't exist
    """
    if not JSONSCHEMA_AVAILABLE:
        return  # Skip validation if jsonschema not installed

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, "r", encoding="utf-8") as fh:
        schema = json.load(fh)

    jsonschema.validate(instance=config, schema=schema)


def validate_config_with_pydantic(config: Dict[str, Any]) -> BatchConfig:
    """Validate configuration using Pydantic model.

    Args:
        config: The configuration dictionary to validate

    Returns:
        Validated BatchConfig model instance

    Raises:
        pydantic.ValidationError: If validation fails
    """
    if not PYDANTIC_AVAILABLE:
        # Return a simple dict-like object if Pydantic not available
        class SimpleConfig:
            def __init__(self, data: Dict[str, Any]):
                self.__dict__.update(data)
        return SimpleConfig(config)  # type: ignore

    return BatchConfig(**config)
