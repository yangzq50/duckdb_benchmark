"""Tests for duckdb_benchmark.cli module."""

import json
import pytest
from pathlib import Path

from duckdb_benchmark.cli import main, create_parser


class TestCLIParser:
    """Tests for CLI argument parsing."""
    
    def test_version_flag(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test --version flag shows version."""
        with pytest.raises(SystemExit) as exc_info:
            main(["--version"])
        
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "0.1.0" in captured.out
    
    def test_no_command_shows_help(self) -> None:
        """Test that running without command returns 0."""
        result = main([])
        assert result == 0
    
    def test_generate_requires_config(self) -> None:
        """Test that generate command requires --config."""
        with pytest.raises(SystemExit) as exc_info:
            main(["generate"])
        
        assert exc_info.value.code == 2
    
    def test_run_requires_config(self) -> None:
        """Test that run command requires --config."""
        with pytest.raises(SystemExit) as exc_info:
            main(["run"])
        
        assert exc_info.value.code == 2
    
    def test_init_requires_output(self) -> None:
        """Test that init command requires --output."""
        with pytest.raises(SystemExit) as exc_info:
            main(["init"])
        
        assert exc_info.value.code == 2


class TestCLICommands:
    """Tests for CLI command execution."""
    
    def test_init_creates_config_file(self, tmp_path: Path) -> None:
        """Test that init command creates a valid sample config."""
        output_file = tmp_path / "sample_config.json"
        
        result = main(["init", "--output", str(output_file)])
        
        assert result == 0
        assert output_file.exists()
        
        with open(output_file) as f:
            config = json.load(f)
        
        assert "scale_factor" in config
        assert "data_path" in config
        assert "output_path" in config
        assert "iterations" in config
        assert "queries" in config
        assert "load_tpch_extension" in config
        assert "in_memory" in config
    
    def test_generate_with_valid_config_returns_error_not_implemented(
        self, tmp_path: Path
    ) -> None:
        """Test generate with valid config returns error (not implemented)."""
        config_data = {
            "scale_factor": 1.0,
            "data_path": str(tmp_path / "data"),
            "output_path": str(tmp_path / "results"),
            "iterations": 1,
            "queries": [1],
            "load_tpch_extension": True,
            "in_memory": True,
        }
        
        config_file = tmp_path / "config.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)
        
        result = main(["generate", "--config", str(config_file)])
        
        # Should return 1 because generation is not implemented
        assert result == 1
    
    def test_run_with_valid_config_returns_error_not_implemented(
        self, tmp_path: Path
    ) -> None:
        """Test run with valid config returns error (not implemented)."""
        config_data = {
            "scale_factor": 1.0,
            "data_path": str(tmp_path / "data"),
            "output_path": str(tmp_path / "results"),
            "iterations": 1,
            "queries": [1],
            "load_tpch_extension": True,
            "in_memory": True,
        }
        
        config_file = tmp_path / "config.json"
        with open(config_file, "w") as f:
            json.dump(config_data, f)
        
        result = main(["run", "--config", str(config_file)])
        
        # Should return 1 because benchmark is not implemented
        assert result == 1
    
    def test_generate_with_invalid_config_returns_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test generate with invalid config file returns error."""
        config_file = tmp_path / "config.json"
        config_file.write_text("invalid json")
        
        result = main(["generate", "--config", str(config_file)])
        
        assert result == 1
        captured = capsys.readouterr()
        assert "Error" in captured.err
