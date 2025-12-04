"""Tests for duckdb_benchmark.data_generator module."""

from pathlib import Path

import duckdb
import pytest

from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.data_generator import DataGenerator, _get_db_filename


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


class TestGetDbFilename:
    """Tests for _get_db_filename function."""

    def test_integer_scale_factor(self) -> None:
        """Test filename generation with integer scale factor."""
        assert _get_db_filename(1.0) == "tpch_sf1.db"
        assert _get_db_filename(10.0) == "tpch_sf10.db"
        assert _get_db_filename(100.0) == "tpch_sf100.db"

    def test_fractional_scale_factor(self) -> None:
        """Test filename generation with fractional scale factor."""
        assert _get_db_filename(0.1) == "tpch_sf0_1.db"
        assert _get_db_filename(0.01) == "tpch_sf0_01.db"


class TestDataGenerator:
    """Tests for DataGenerator class."""

    @pytest.fixture
    def config(self, tmp_path: Path) -> BenchmarkConfig:
        """Create a valid test configuration."""
        return BenchmarkConfig(
            scale_factor=0.01,  # Very small for fast tests
            data_path=tmp_path / "data",
            output_path=tmp_path / "results",
            iterations=1,
            queries=[1],
            tpch_extension_path=None,
        )

    def test_generator_initialization(self, config: BenchmarkConfig) -> None:
        """Test that DataGenerator initializes with config."""
        generator = DataGenerator(config)
        assert generator.config == config

    def test_data_exists_returns_false_for_empty_path(self, config: BenchmarkConfig) -> None:
        """Test data_exists returns False when database file doesn't exist."""
        generator = DataGenerator(config)
        assert generator.data_exists() is False

    def test_data_exists_returns_true_when_db_exists(self, config: BenchmarkConfig) -> None:
        """Test data_exists returns True when database file exists."""
        config.data_path.mkdir(parents=True)
        db_path = config.data_path / _get_db_filename(config.scale_factor)
        db_path.touch()

        generator = DataGenerator(config)
        assert generator.data_exists() is True

    def test_get_db_path(self, config: BenchmarkConfig) -> None:
        """Test get_db_path returns correct path."""
        generator = DataGenerator(config)
        expected_path = config.data_path / "tpch_sf0_01.db"
        assert generator.get_db_path() == expected_path

    @pytest.mark.skipif(not TPCH_AVAILABLE, reason="TPCH extension not available")
    def test_generate_creates_database(self, config: BenchmarkConfig) -> None:
        """Test generate creates database file."""
        generator = DataGenerator(config)
        db_path = generator.generate()

        assert db_path.exists()
        assert db_path == generator.get_db_path()

    def test_generate_raises_if_file_exists(self, config: BenchmarkConfig) -> None:
        """Test generate raises FileExistsError if database already exists."""
        config.data_path.mkdir(parents=True)
        db_path = config.data_path / _get_db_filename(config.scale_factor)
        db_path.touch()

        generator = DataGenerator(config)

        with pytest.raises(FileExistsError, match="already exists"):
            generator.generate()
