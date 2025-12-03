"""Tests for duckdb_benchmark.load_tpch_extension module."""

import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from duckdb_benchmark.load_tpch_extension import (
    _escape_sql_string,
    _get_default_extension_path,
    _get_duckdb_version,
    _get_extension_download_url,
    _get_platform,
    download_tpch_extension,
    load_tpch,
    load_tpch_extension_from_path,
)


class TestEscapeSqlString:
    """Tests for _escape_sql_string function."""

    def test_escapes_single_quotes(self) -> None:
        """Test that single quotes are escaped."""
        assert _escape_sql_string("it's") == "it''s"
        assert _escape_sql_string("'test'") == "''test''"

    def test_no_quotes_unchanged(self) -> None:
        """Test that strings without quotes are unchanged."""
        assert _escape_sql_string("test") == "test"
        assert _escape_sql_string("/path/to/file") == "/path/to/file"


class TestGetDefaultExtensionPath:
    """Tests for _get_default_extension_path function."""

    def test_returns_correct_path(self, tmp_path: Path) -> None:
        """Test that default path is data_path/tpch.duckdb_extension."""
        expected = tmp_path / "tpch.duckdb_extension"
        assert _get_default_extension_path(tmp_path) == expected


class TestGetDuckdbVersion:
    """Tests for _get_duckdb_version function."""

    def test_returns_duckdb_version(self) -> None:
        """Test that it returns the DuckDB version string."""
        version = _get_duckdb_version()
        assert isinstance(version, str)
        assert version == duckdb.__version__


class TestGetPlatform:
    """Tests for _get_platform function."""

    def test_returns_string(self) -> None:
        """Test that it returns a string."""
        platform = _get_platform()
        assert isinstance(platform, str)
        assert "_" in platform  # Should be format like "linux_amd64"


class TestGetExtensionDownloadUrl:
    """Tests for _get_extension_download_url function."""

    def test_returns_correct_url_format(self) -> None:
        """Test that URL has correct format with explicit platform."""
        url = _get_extension_download_url("1.0.0", "linux_amd64")
        assert url == "http://extensions.duckdb.org/v1.0.0/linux_amd64/tpch.duckdb_extension.gz"

    def test_url_includes_version(self) -> None:
        """Test that URL includes provided version."""
        url = _get_extension_download_url("1.2.3", "linux_amd64")
        assert "v1.2.3" in url

    def test_url_includes_platform(self) -> None:
        """Test that URL includes the specified platform."""
        url = _get_extension_download_url("1.0.0", "osx_arm64")
        assert "osx_arm64" in url


class TestDownloadTpchExtension:
    """Tests for download_tpch_extension function."""

    def test_downloads_and_extracts(self, tmp_path: Path) -> None:
        """Test downloading and extracting extension (mocked)."""
        extension_path = tmp_path / "tpch.duckdb_extension"
        gz_path = Path(str(extension_path) + ".gz")

        # Mock urlretrieve to create the gz file
        test_content = b"test extension content"
        with patch("duckdb_benchmark.load_tpch_extension.urllib.request.urlretrieve") as mock_retrieve:
            def create_gz_file(url: str, path: str) -> None:
                with gzip.open(path, "wb") as f:
                    f.write(test_content)

            mock_retrieve.side_effect = create_gz_file

            result = download_tpch_extension(extension_path, "1.0.0")

            assert result == extension_path
            assert extension_path.exists()
            assert extension_path.read_bytes() == test_content
            assert not gz_path.exists()  # gz file should be cleaned up

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        """Test that parent directory is created if it doesn't exist."""
        extension_path = tmp_path / "subdir" / "tpch.duckdb_extension"

        test_content = b"test content"

        with patch("duckdb_benchmark.load_tpch_extension.urllib.request.urlretrieve") as mock_retrieve:
            def create_gz_file(url: str, path: str) -> None:
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(path, "wb") as f:
                    f.write(test_content)

            mock_retrieve.side_effect = create_gz_file

            result = download_tpch_extension(extension_path, "1.0.0")

            assert result == extension_path
            assert extension_path.parent.exists()

    def test_uses_current_version_when_not_provided(self, tmp_path: Path) -> None:
        """Test that current DuckDB version is used when not provided."""
        extension_path = tmp_path / "tpch.duckdb_extension"
        test_content = b"test"

        with patch("duckdb_benchmark.load_tpch_extension.urllib.request.urlretrieve") as mock_retrieve:
            def create_gz_file(url: str, path: str) -> None:
                assert f"v{duckdb.__version__}" in url
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(path, "wb") as f:
                    f.write(test_content)

            mock_retrieve.side_effect = create_gz_file

            download_tpch_extension(extension_path)


class TestLoadTpch:
    """Tests for load_tpch function."""

    def test_uses_bundled_extension_when_no_paths_provided(self) -> None:
        """Test that bundled extension is used when no paths provided."""
        conn = MagicMock()

        load_tpch(conn)

        # Should call INSTALL tpch; and LOAD tpch;
        conn.execute.assert_any_call("INSTALL tpch;")
        conn.execute.assert_any_call("LOAD tpch;")

    def test_uses_custom_extension_path(self, tmp_path: Path) -> None:
        """Test that custom extension path is used when provided."""
        extension_file = tmp_path / "custom" / "tpch.duckdb_extension"
        extension_file.parent.mkdir(parents=True)
        extension_file.touch()

        conn = MagicMock()

        load_tpch(conn, extension_path=extension_file)

        # Should load directly from the custom path
        expected_load = f"LOAD '{extension_file}';"
        conn.execute.assert_called_once_with(expected_load)

    def test_uses_default_path_when_data_path_provided(self, tmp_path: Path) -> None:
        """Test that default path within data_path is used."""
        default_ext = tmp_path / "tpch.duckdb_extension"
        default_ext.touch()

        conn = MagicMock()

        load_tpch(conn, data_path=tmp_path)

        # Should load from the default path
        expected_load = f"LOAD '{default_ext}';"
        conn.execute.assert_called_once_with(expected_load)

    def test_downloads_when_default_not_exists(self, tmp_path: Path) -> None:
        """Test download is called when default path doesn't exist."""
        conn = MagicMock()

        # data_path is provided but default extension file doesn't exist
        with patch("duckdb_benchmark.load_tpch_extension.download_tpch_extension") as mock_download:
            # Make download create the file
            def create_file(path: Path) -> Path:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
                return path

            mock_download.side_effect = create_file

            load_tpch(conn, data_path=tmp_path)

            # Should have called download
            expected_path = tmp_path / "tpch.duckdb_extension"
            mock_download.assert_called_once_with(expected_path)

            # Should load from the path
            expected_load = f"LOAD '{expected_path}';"
            conn.execute.assert_called_once_with(expected_load)

    def test_extension_path_takes_precedence(self, tmp_path: Path) -> None:
        """Test that extension_path takes precedence over data_path default."""
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        custom_ext = custom_dir / "tpch.duckdb_extension"
        custom_ext.touch()

        # Also create default in data_path
        default_ext = tmp_path / "tpch.duckdb_extension"
        default_ext.touch()

        conn = MagicMock()

        load_tpch(conn, extension_path=custom_ext, data_path=tmp_path)

        # Should use the custom path, not the default
        expected_load = f"LOAD '{custom_ext}';"
        conn.execute.assert_called_once_with(expected_load)


class TestLoadTpchExtensionFromPath:
    """Tests for load_tpch_extension_from_path function."""

    def test_raises_when_file_not_found(self, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised when file doesn't exist."""
        conn = MagicMock()
        nonexistent = tmp_path / "nonexistent.duckdb_extension"

        with pytest.raises(FileNotFoundError, match="Extension file not found"):
            load_tpch_extension_from_path(conn, nonexistent)

    def test_loads_from_path(self, tmp_path: Path) -> None:
        """Test that extension is loaded using LOAD command."""
        extension_file = tmp_path / "tpch.duckdb_extension"
        extension_file.touch()

        conn = MagicMock()

        load_tpch_extension_from_path(conn, extension_file)

        expected_load = f"LOAD '{extension_file}';"
        conn.execute.assert_called_once_with(expected_load)

    def test_escapes_path_with_quotes(self, tmp_path: Path) -> None:
        """Test that path with single quotes is properly escaped."""
        # Create a directory with a quote in the name
        special_dir = tmp_path / "test'dir"
        special_dir.mkdir()
        extension_file = special_dir / "tpch.duckdb_extension"
        extension_file.touch()

        conn = MagicMock()

        load_tpch_extension_from_path(conn, extension_file)

        # The path should have escaped quotes
        call_arg = conn.execute.call_args[0][0]
        assert "''" in call_arg
