"""Tests for duckdb_benchmark.data_generator module."""

import pytest
from pathlib import Path

from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.data_generator import DataGenerator


class TestDataGenerator:
    """Tests for DataGenerator class."""
    
    @pytest.fixture
    def config(self, tmp_path: Path) -> BenchmarkConfig:
        """Create a valid test configuration."""
        return BenchmarkConfig(
            scale_factor=1.0,
            data_path=tmp_path / "data",
            output_path=tmp_path / "results",
            iterations=1,
            queries=[1],
            load_tpch_extension=True,
            in_memory=True,
        )
    
    def test_generator_initialization(self, config: BenchmarkConfig) -> None:
        """Test that DataGenerator initializes with config."""
        generator = DataGenerator(config)
        assert generator.config == config
    
    def test_data_exists_returns_false_for_empty_path(
        self, config: BenchmarkConfig
    ) -> None:
        """Test data_exists returns False when path doesn't exist."""
        generator = DataGenerator(config)
        assert generator.data_exists() is False
    
    def test_data_exists_returns_false_for_empty_dir(
        self, config: BenchmarkConfig
    ) -> None:
        """Test data_exists returns False for empty directory."""
        config.data_path.mkdir(parents=True)
        generator = DataGenerator(config)
        assert generator.data_exists() is False
    
    def test_data_exists_returns_true_when_files_exist(
        self, config: BenchmarkConfig
    ) -> None:
        """Test data_exists returns True when files exist."""
        config.data_path.mkdir(parents=True)
        (config.data_path / "test.parquet").touch()
        
        generator = DataGenerator(config)
        assert generator.data_exists() is True
    
    def test_generate_raises_not_implemented(
        self, config: BenchmarkConfig
    ) -> None:
        """Test generate raises NotImplementedError."""
        generator = DataGenerator(config)
        
        with pytest.raises(NotImplementedError):
            generator.generate()
