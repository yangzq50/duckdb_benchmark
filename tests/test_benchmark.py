"""Tests for duckdb_benchmark.benchmark module."""

import json
import statistics
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

    def test_compute_summary_includes_new_statistics(
        self, config: BenchmarkConfig
    ) -> None:
        """Test that _compute_summary includes median, stdev, and variance."""
        benchmark = Benchmark(config)

        # Manually populate results to test summary computation
        times = [100.0, 200.0, 150.0, 250.0, 175.0]
        benchmark.results = [
            BenchmarkResult(
                query_number=1,
                iteration=i + 1,
                execution_time_ms=t,
                row_count=10,
                success=True,
            )
            for i, t in enumerate(times)
        ]

        summary = benchmark._compute_summary()

        # Check that all new statistics are present
        assert "query_1" in summary
        query_summary = summary["query_1"]

        # Check existing keys
        assert "min_ms" in query_summary
        assert "max_ms" in query_summary
        assert "avg_ms" in query_summary
        assert "iterations" in query_summary
        assert "all_success" in query_summary

        # Check new keys
        assert "median_ms" in query_summary
        assert "stdev_ms" in query_summary
        assert "variance_ms" in query_summary

        # Verify values are correct
        assert query_summary["min_ms"] == 100.0
        assert query_summary["max_ms"] == 250.0
        assert query_summary["avg_ms"] == sum(times) / len(times)
        assert query_summary["median_ms"] == statistics.median(times)
        assert query_summary["stdev_ms"] == statistics.stdev(times)
        assert query_summary["variance_ms"] == statistics.variance(times)
        assert query_summary["iterations"] == 5
        assert query_summary["all_success"] is True

    def test_compute_summary_single_result(self, config: BenchmarkConfig) -> None:
        """Test _compute_summary handles single result (stdev/variance should be 0)."""
        benchmark = Benchmark(config)

        # Single result
        benchmark.results = [
            BenchmarkResult(
                query_number=1,
                iteration=1,
                execution_time_ms=100.0,
                row_count=10,
                success=True,
            )
        ]

        summary = benchmark._compute_summary()
        query_summary = summary["query_1"]

        # With single result, stdev and variance should be 0.0
        assert query_summary["stdev_ms"] == 0.0
        assert query_summary["variance_ms"] == 0.0
        assert query_summary["median_ms"] == 100.0

    def test_compute_summary_no_results(self, config: BenchmarkConfig) -> None:
        """Test _compute_summary handles no results (None values)."""
        benchmark = Benchmark(config)
        benchmark.results = []

        summary = benchmark._compute_summary()
        query_summary = summary["query_1"]

        # With no results, all values should be None or False
        assert query_summary["min_ms"] is None
        assert query_summary["max_ms"] is None
        assert query_summary["avg_ms"] is None
        assert query_summary["median_ms"] is None
        assert query_summary["stdev_ms"] is None
        assert query_summary["variance_ms"] is None
        assert query_summary["iterations"] == 0
        assert query_summary["all_success"] is False
