"""
Data generation module for duckdb_benchmark.

Provides TPC-H data generation logic using DuckDB's TPC-H extension.
"""

from pathlib import Path

import duckdb

from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.load_tpch_extension import install_and_load_tpch


def _escape_sql_string(value: str) -> str:
    """
    Escape a string value for safe use in SQL.

    Replaces single quotes with escaped single quotes to prevent SQL injection.

    Args:
        value: The string to escape

    Returns:
        Escaped string safe for SQL interpolation
    """
    return value.replace("'", "''")


def _format_scale_factor(scale_factor: float) -> str:
    """
    Format scale factor as a string suitable for filenames.

    Converts dots to underscores for fractional scale factors
    (e.g., 0.1 -> '0_1', 1.0 -> '1').

    Args:
        scale_factor: The TPC-H scale factor

    Returns:
        Formatted string representation of scale factor
    """
    # Use is_integer() for cleaner float-to-integer detection
    if float(scale_factor).is_integer():
        return str(int(scale_factor))
    else:
        return str(scale_factor).replace(".", "_")


def _get_db_filename(scale_factor: float) -> str:
    """
    Get the database filename with scale factor included.

    Args:
        scale_factor: The TPC-H scale factor

    Returns:
        Database filename string like 'tpch_sf1.db'
    """
    return f"tpch_sf{_format_scale_factor(scale_factor)}.db"


class DataGenerator:
    """
    TPC-H data generator for DuckDB benchmarks.

    This class handles the generation and persistence of TPC-H benchmark data.
    DuckDB runs in-memory only, and data is persisted to disk using ATTACH/COPY/DETACH.
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        """
        Initialize the data generator.

        Args:
            config: Benchmark configuration specifying scale factor and paths
        """
        self.config = config

    def _get_db_path(self) -> Path:
        """Get the path to the persistent database file."""
        return self.config.data_path / _get_db_filename(self.config.scale_factor)

    def _install_and_load_tpch(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Install and load the TPC-H extension.

        Args:
            conn: DuckDB connection

        Raises:
            duckdb.Error: If extension installation or loading fails
        """
        install_and_load_tpch(
            conn,
            extension_path=self.config.tpch_extension_path,
            data_path=self.config.data_path,
        )

    def generate(self) -> Path:
        """
        Generate TPC-H data based on configuration.

        DuckDB runs in-memory only. Data is generated using dbgen() and then
        persisted to disk using the ATTACH/COPY/DETACH pattern.

        Returns:
            Path to the generated database file

        Raises:
            FileExistsError: If the database file already exists
            duckdb.Error: If data generation fails
        """
        # Ensure data directory exists
        self.config.data_path.mkdir(parents=True, exist_ok=True)

        db_path = self._get_db_path()

        # Ensure the file does NOT exist before attaching (as per requirements)
        if db_path.exists():
            raise FileExistsError(
                f"Database file already exists: {db_path}. "
                "Delete it first or use a different data_path."
            )

        # Create in-memory connection
        conn = duckdb.connect(":memory:")

        try:
            # Install and load TPC-H extension
            self._install_and_load_tpch(conn)

            # Generate TPC-H data in memory
            conn.execute(f"CALL dbgen(sf = {self.config.scale_factor});")

            # Persist data to disk using ATTACH/COPY/DETACH pattern
            # Use a safe database alias (not "my_database")
            db_alias = "tpch_persist"
            escaped_db_path = _escape_sql_string(str(db_path))
            conn.execute(f"ATTACH '{escaped_db_path}' AS {db_alias};")
            conn.execute(f"COPY FROM DATABASE memory TO {db_alias};")
            conn.execute(f"DETACH {db_alias};")

        finally:
            conn.close()

        return db_path

    def data_exists(self) -> bool:
        """
        Check if TPC-H data already exists at the configured path.

        Returns:
            True if the database file exists, False otherwise
        """
        return self._get_db_path().exists()

    def get_db_path(self) -> Path:
        """
        Get the path to the database file for the configured scale factor.

        Returns:
            Path to the database file
        """
        return self._get_db_path()
