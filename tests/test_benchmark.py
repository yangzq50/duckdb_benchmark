"""Tests for duckdb_benchmark.benchmark module."""

import json
from pathlib import Path

import duckdb
import pytest

from duckdb_benchmark.benchmark import Benchmark, BenchmarkResult
from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.data_generator import DataGenerator


def tpch_extension_available() -> bool:
    """Check if the TPCH extension can be installed."""
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL tpch;")
        conn.execute("LOAD tpch;")
        conn.close()
        return True
    except Exception:
        return False


# Check once at module load
TPCH_AVAILABLE = tpch_extension_available()


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
            scale_factor=0.01,  # Very small for fast tests
            data_path=tmp_path / "data",
            output_path=tmp_path / "results",
            iterations=2,
            queries=[1],
            tpch_extension_path=None,
        )

    @pytest.fixture
    def config_with_data(self, config: BenchmarkConfig) -> BenchmarkConfig:
        """Create config with generated data."""
        generator = DataGenerator(config)
        generator.generate()
        return config

    def test_benchmark_initialization(self, config: BenchmarkConfig) -> None:
        """Test that Benchmark initializes with config."""
        benchmark = Benchmark(config)
        assert benchmark.config == config
        assert benchmark.results == []

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_run_raises_file_not_found_without_data(
        self, config: BenchmarkConfig
    ) -> None:
        """Test run raises FileNotFoundError when data doesn't exist."""
        benchmark = Benchmark(config)

        with pytest.raises(FileNotFoundError, match="Database file not found"):
            benchmark.run()

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_run_executes_queries(self, config_with_data: BenchmarkConfig) -> None:
        """Test run executes configured queries."""
        benchmark = Benchmark(config_with_data)
        results = benchmark.run()

        # Should have 2 results (1 query * 2 iterations)
        assert len(results) == 2
        assert all(r.query_number == 1 for r in results)
        assert all(r.success for r in results)
        assert results[0].iteration == 1
        assert results[1].iteration == 2

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_run_populates_results(self, config_with_data: BenchmarkConfig) -> None:
        """Test that run populates the results attribute."""
        benchmark = Benchmark(config_with_data)
        benchmark.run()

        assert len(benchmark.results) == 2

    def test_save_results_raises_without_results(
        self, config: BenchmarkConfig
    ) -> None:
        """Test save_results raises ValueError when no results."""
        benchmark = Benchmark(config)

        with pytest.raises(ValueError, match="No benchmark results"):
            benchmark.save_results()

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_save_results_creates_file(
        self, config_with_data: BenchmarkConfig
    ) -> None:
        """Test save_results creates output file."""
        benchmark = Benchmark(config_with_data)
        benchmark.run()
        output_file = benchmark.save_results()

        assert output_file.exists()
        assert output_file.suffix == ".json"
        assert "sf0_01" in output_file.name

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_save_results_content(self, config_with_data: BenchmarkConfig) -> None:
        """Test save_results file has correct content."""
        benchmark = Benchmark(config_with_data)
        benchmark.run()
        output_file = benchmark.save_results()

        with open(output_file) as f:
            data = json.load(f)

        assert "config" in data
        assert "timestamp" in data
        assert "results" in data
        assert "summary" in data
        assert data["config"]["scale_factor"] == 0.01
        assert len(data["results"]) == 2
