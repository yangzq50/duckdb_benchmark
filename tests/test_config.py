"""Tests for duckdb_benchmark.config module."""

import json
import pytest
from pathlib import Path

from duckdb_benchmark.config import BenchmarkConfig, load_config


class TestBenchmarkConfig:
    """Tests for BenchmarkConfig class."""
    
    def test_valid_config_creation(self) -> None:
        """Test creating a valid configuration."""
        config = BenchmarkConfig(
            scale_factor=1.0,
            data_path=Path("./data"),
            output_path=Path("./results"),
            iterations=3,
            queries=[1, 2, 3],
            load_tpch_extension=True,
            in_memory=True,
        )
        
        assert config.scale_factor == 1.0
        assert config.data_path == Path("./data")
        assert config.output_path == Path("./results")
        assert config.iterations == 3
        assert config.queries == [1, 2, 3]
        assert config.load_tpch_extension is True
        assert config.in_memory is True
    
    def test_path_conversion(self) -> None:
        """Test that string paths are converted to Path objects."""
        config = BenchmarkConfig(
            scale_factor=1.0,
            data_path="./data",  # type: ignore
            output_path="./results",  # type: ignore
            iterations=1,
            queries=[1],
            load_tpch_extension=True,
            in_memory=True,
        )
        
        assert isinstance(config.data_path, Path)
        assert isinstance(config.output_path, Path)
    
    def test_invalid_scale_factor_raises(self) -> None:
        """Test that negative scale factor raises ValueError."""
        with pytest.raises(ValueError, match="scale_factor must be positive"):
            BenchmarkConfig(
                scale_factor=-1.0,
                data_path=Path("./data"),
                output_path=Path("./results"),
                iterations=1,
                queries=[1],
                load_tpch_extension=True,
                in_memory=True,
            )
    
    def test_invalid_iterations_raises(self) -> None:
        """Test that zero iterations raises ValueError."""
        with pytest.raises(ValueError, match="iterations must be positive"):
            BenchmarkConfig(
                scale_factor=1.0,
                data_path=Path("./data"),
                output_path=Path("./results"),
                iterations=0,
                queries=[1],
                load_tpch_extension=True,
                in_memory=True,
            )
    
    def test_empty_queries_raises(self) -> None:
        """Test that empty queries list raises ValueError."""
        with pytest.raises(ValueError, match="queries list cannot be empty"):
            BenchmarkConfig(
                scale_factor=1.0,
                data_path=Path("./data"),
                output_path=Path("./results"),
                iterations=1,
                queries=[],
                load_tpch_extension=True,
                in_memory=True,
            )
    
    def test_invalid_query_number_raises(self) -> None:
        """Test that query number outside 1-22 raises ValueError."""
        with pytest.raises(ValueError, match="query 23 must be between 1 and 22"):
            BenchmarkConfig(
                scale_factor=1.0,
                data_path=Path("./data"),
                output_path=Path("./results"),
                iterations=1,
                queries=[1, 23],
                load_tpch_extension=True,
                in_memory=True,
            )


class TestLoadConfig:
    """Tests for load_config function."""
    
    def test_load_valid_config(self, tmp_path: Path) -> None:
        """Test loading a valid configuration file."""
        config_data = {
            "scale_factor": 10.0,
            "data_path": "./data",
            "output_path": "./results",
            "iterations": 5,
            "queries": [1, 5, 10],
            "load_tpch_extension": False,
            "in_memory": False,
        }
        
        config_file = tmp_path / "config.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)
        
        config = load_config(config_file)
        
        assert config.scale_factor == 10.0
        assert config.iterations == 5
        assert config.queries == [1, 5, 10]
        assert config.load_tpch_extension is False
    
    def test_load_missing_file_raises(self) -> None:
        """Test that loading non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.json"))
    
    def test_load_invalid_json_raises(self, tmp_path: Path) -> None:
        """Test that loading invalid JSON raises JSONDecodeError."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("not valid json")
        
        with pytest.raises(json.JSONDecodeError):
            load_config(config_file)
    
    def test_load_missing_field_raises(self, tmp_path: Path) -> None:
        """Test that missing required field raises KeyError."""
        config_data = {
            "scale_factor": 1.0,
            # Missing other required fields
        }
        
        config_file = tmp_path / "incomplete.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(KeyError):
            load_config(config_file)
