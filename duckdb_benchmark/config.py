"""
Configuration module for duckdb_benchmark.

Provides a configuration class and loader with no hidden defaults.
All configuration values must be explicitly provided.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import json


@dataclass
class BenchmarkConfig:
    """
    Configuration for DuckDB TPC-H benchmarks.
    
    All fields are required - no hidden defaults to ensure explicit configuration.
    
    Attributes:
        scale_factor: TPC-H scale factor (e.g., 1, 10, 100)
        data_path: Path where TPC-H data files will be stored
        output_path: Path where benchmark results will be written
        iterations: Number of benchmark iterations per query
        queries: List of TPC-H query numbers to run (1-22)
        tpch_extension_path: Optional path to TPCH extension file; if None, uses bundled extension
    """
    scale_factor: float
    data_path: Path
    output_path: Path
    iterations: int
    queries: list[int]
    tpch_extension_path: Optional[Path]
    
    def __post_init__(self) -> None:
        """Validate and convert configuration values."""
        self.data_path = Path(self.data_path)
        self.output_path = Path(self.output_path)
        if self.tpch_extension_path is not None:
            self.tpch_extension_path = Path(self.tpch_extension_path)
        
        # Validate types and values
        if not isinstance(self.scale_factor, (int, float)):
            raise TypeError("scale_factor must be a number")
        if self.scale_factor <= 0:
            raise ValueError("scale_factor must be positive")
        if not isinstance(self.iterations, int):
            raise TypeError("iterations must be an integer")
        if self.iterations <= 0:
            raise ValueError("iterations must be positive")
        if not self.queries:
            raise ValueError("queries list cannot be empty")
        for q in self.queries:
            if not isinstance(q, int):
                raise TypeError(f"query {q} must be an integer")
            if not 1 <= q <= 22:
                raise ValueError(f"query {q} must be between 1 and 22")
        if self.tpch_extension_path is not None and not self.tpch_extension_path.exists():
            raise ValueError(f"tpch_extension_path does not exist: {self.tpch_extension_path}")


def load_config(config_path: Path) -> BenchmarkConfig:
    """
    Load benchmark configuration from a JSON file.
    
    Args:
        config_path: Path to the JSON configuration file
        
    Returns:
        BenchmarkConfig instance with loaded values
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is not valid JSON
        TypeError: If required fields are missing
        ValueError: If field values are invalid
    """
    with open(config_path, "r") as f:
        data = json.load(f)
    
    tpch_extension_path = data.get("tpch_extension_path")
    if tpch_extension_path is not None:
        tpch_extension_path = Path(tpch_extension_path)
    
    return BenchmarkConfig(
        scale_factor=data["scale_factor"],
        data_path=Path(data["data_path"]),
        output_path=Path(data["output_path"]),
        iterations=data["iterations"],
        queries=data["queries"],
        tpch_extension_path=tpch_extension_path,
    )
