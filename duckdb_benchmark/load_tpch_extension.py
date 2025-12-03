"""
TPC-H extension loader module for duckdb_benchmark.

Provides centralized logic for downloading, installing, and loading
the DuckDB TPC-H extension.
"""

import gzip
import shutil
import urllib.request
from pathlib import Path
from typing import Optional

import duckdb


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


def _get_default_extension_path(data_path: Path) -> Path:
    """
    Get the default path for the TPC-H extension.

    Args:
        data_path: Base data directory path

    Returns:
        Path to the default TPC-H extension file
    """
    return data_path / "tpch.duckdb_extension"


def _get_duckdb_version() -> str:
    """
    Get the current DuckDB version.

    Returns:
        Version string (e.g., "1.4.2")
    """
    return duckdb.__version__


def _get_platform() -> str:
    """
    Get the platform identifier for extension downloads.

    Returns:
        Platform identifier string (e.g., "linux_amd64", "osx_arm64")
    """
    import platform
    import sys

    system = platform.system().lower()
    machine = platform.machine().lower()

    # Map system names
    if system == "darwin":
        system = "osx"

    # Map architecture names
    arch_map = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }
    arch = arch_map.get(machine, machine)

    return f"{system}_{arch}"


def _get_extension_download_url(
    duckdb_version: str,
    platform: Optional[str] = None,
) -> str:
    """
    Get the download URL for the TPC-H extension.

    Args:
        duckdb_version: DuckDB version string
        platform: Optional platform identifier (e.g., "linux_amd64")
                  If not provided, auto-detected from the current system

    Returns:
        URL to download the TPC-H extension
    """
    if platform is None:
        platform = _get_platform()

    return (
        f"http://extensions.duckdb.org/v{duckdb_version}/"
        f"{platform}/tpch.duckdb_extension.gz"
    )


def download_tpch_extension(
    extension_path: Path,
    duckdb_version: Optional[str] = None,
    platform: Optional[str] = None,
) -> Path:
    """
    Download and uncompress the TPC-H extension.

    Downloads the TPC-H extension from the DuckDB extension repository
    and uncompresses it to the specified path.

    Args:
        extension_path: Path where the uncompressed extension will be saved
        duckdb_version: Optional DuckDB version; uses current version if not provided
        platform: Optional platform identifier (e.g., "linux_amd64")
                  If not provided, auto-detected from the current system

    Returns:
        Path to the downloaded and uncompressed extension file

    Raises:
        urllib.error.URLError: If download fails
        OSError: If file operations fail
    """
    if duckdb_version is None:
        duckdb_version = _get_duckdb_version()

    url = _get_extension_download_url(duckdb_version, platform)

    # Ensure parent directory exists
    extension_path.parent.mkdir(parents=True, exist_ok=True)

    # Download the compressed extension
    # Use string concatenation to ensure we add .gz suffix correctly
    gz_path = Path(str(extension_path) + ".gz")

    urllib.request.urlretrieve(url, gz_path)

    # Uncompress the extension
    with gzip.open(gz_path, "rb") as f_in:
        with open(extension_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Clean up the compressed file
    gz_path.unlink()

    return extension_path


def install_and_load_tpch(
    conn: duckdb.DuckDBPyConnection,
    extension_path: Optional[Path] = None,
    data_path: Optional[Path] = None,
) -> None:
    """
    Install and load the TPC-H extension.

    If extension_path is provided, installs from that path.
    If extension_path is None but data_path is provided, uses the default path
    within data_path (data_path/tpch.duckdb_extension).
    If neither is provided, uses the bundled DuckDB extension.

    Args:
        conn: DuckDB connection
        extension_path: Optional explicit path to TPC-H extension file
        data_path: Optional data directory path for default extension location

    Raises:
        duckdb.Error: If extension installation or loading fails
    """
    # Determine the effective extension path
    effective_path = extension_path
    if effective_path is None and data_path is not None:
        effective_path = _get_default_extension_path(data_path)

    # Install extension (uses custom path if provided)
    if effective_path is not None and effective_path.exists():
        escaped_path = _escape_sql_string(str(effective_path.parent))
        conn.execute(f"INSTALL tpch FROM '{escaped_path}';")
    else:
        conn.execute("INSTALL tpch;")

    # Load extension (always load)
    conn.execute("LOAD tpch;")


def load_tpch_extension_from_path(
    conn: duckdb.DuckDBPyConnection,
    extension_path: Path,
) -> None:
    """
    Load the TPC-H extension directly from a specific file path.

    This uses LOAD with the full path rather than INSTALL + LOAD.

    Args:
        conn: DuckDB connection
        extension_path: Path to the TPC-H extension file

    Raises:
        FileNotFoundError: If extension file doesn't exist
        duckdb.Error: If loading fails
    """
    if not extension_path.exists():
        raise FileNotFoundError(f"Extension file not found: {extension_path}")

    escaped_path = _escape_sql_string(str(extension_path))
    conn.execute(f"LOAD '{escaped_path}';")
