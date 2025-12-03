"""Tests for duckdb_benchmark.benchmark module."""

import pytest
from pathlib import Path

from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.benchmark import Benchmark, BenchmarkResult


class TestBenchmarkResult:
    """Tests for BenchmarkResult dataclass."""
    
    def test_result_creation(self) -> None:
        """Test creating a benchmark result."""
        result = BenchmarkResult(
            query_number=1,
            iteration=1,
            execution_time_ms=100.5,
            row_count=10,
            success=True,
        )
        
        assert result.query_number == 1
        assert result.iteration == 1
        assert result.execution_time_ms == 100.5
        assert result.row_count == 10
        assert result.success is True
        assert result.error is None
    
    def test_result_with_error(self) -> None:
        """Test creating a failed benchmark result."""
        result = BenchmarkResult(
            query_number=1,
            iteration=1,
            execution_time_ms=0.0,
            row_count=0,
            success=False,
            error="Query timeout",
        )
        
        assert result.success is False
        assert result.error == "Query timeout"


class TestBenchmark:
    """Tests for Benchmark class."""
    
    @pytest.fixture
    def config(self, tmp_path: Path) -> BenchmarkConfig:
        """Create a valid test configuration."""
        return BenchmarkConfig(
            scale_factor=1.0,
            data_path=tmp_path / "data",
            output_path=tmp_path / "results",
            iterations=3,
            queries=[1, 5, 10],
            load_tpch_extension=True,
            in_memory=True,
        )
    
    def test_benchmark_initialization(self, config: BenchmarkConfig) -> None:
        """Test that Benchmark initializes with config."""
        benchmark = Benchmark(config)
        assert benchmark.config == config
        assert benchmark.results == []
    
    def test_run_raises_not_implemented(self, config: BenchmarkConfig) -> None:
        """Test run raises NotImplementedError."""
        benchmark = Benchmark(config)
        
        with pytest.raises(NotImplementedError):
            benchmark.run()
    
    def test_save_results_raises_not_implemented(
        self, config: BenchmarkConfig
    ) -> None:
        """Test save_results raises NotImplementedError."""
        benchmark = Benchmark(config)
        
        with pytest.raises(NotImplementedError):
            benchmark.save_results()
