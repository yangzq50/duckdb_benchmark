"""Tests for duckdb_benchmark module."""

import duckdb_benchmark
from duckdb_benchmark import (
    Benchmark,
    BenchmarkConfig,
    DataGenerator,
    load_config,
    load_tpch_extension,
)


class TestModuleExports:
    """Tests for module-level exports."""

    def test_version_exists(self) -> None:
        """Test that version is defined."""
        assert hasattr(duckdb_benchmark, "__version__")
        assert duckdb_benchmark.__version__ == "0.1.0"

    def test_exports_benchmarkconfig(self) -> None:
        """Test that BenchmarkConfig is exported."""
        assert BenchmarkConfig is not None

    def test_exports_load_config(self) -> None:
        """Test that load_config is exported."""
        assert callable(load_config)

    def test_exports_datagenerator(self) -> None:
        """Test that DataGenerator is exported."""
        assert DataGenerator is not None

    def test_exports_benchmark(self) -> None:
        """Test that Benchmark is exported."""
        assert Benchmark is not None

    def test_exports_load_tpch_extension(self) -> None:
        """Test that load_tpch_extension is exported."""
        assert callable(load_tpch_extension)

    def test_all_exports_defined(self) -> None:
        """Test that __all__ contains expected exports."""
        expected = [
            "__version__",
            "BenchmarkConfig",
            "load_config",
            "DataGenerator",
            "Benchmark",
            "load_tpch_extension",
        ]
        for name in expected:
            assert name in duckdb_benchmark.__all__
