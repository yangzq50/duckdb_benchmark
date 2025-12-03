"""
Benchmarking module for duckdb_benchmark.

Provides TPC-H benchmarking logic using DuckDB.
"""

import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

from duckdb_benchmark.config import BenchmarkConfig
from duckdb_benchmark.data_generator import (
    _get_db_filename,
    _escape_sql_string,
    _format_scale_factor,
)
from duckdb_benchmark.load_tpch_extension import install_and_load_tpch


@dataclass
class BenchmarkResult:
    """
    Result of a single benchmark query execution.
    
    Attributes:
        query_number: TPC-H query number (1-22)
        iteration: Iteration number
        execution_time_ms: Query execution time in milliseconds
        row_count: Number of rows returned
        success: Whether the query executed successfully
        error: Error message if query failed
    """
    query_number: int
    iteration: int
    execution_time_ms: float
    row_count: int
    success: bool
    error: Optional[str] = None


class Benchmark:
    """
    TPC-H benchmark runner for DuckDB.
    
    This class handles the execution and measurement of TPC-H queries.
    Uses a separate DuckDB connection from data generation and loads
    data into memory using the ATTACH/COPY/DETACH pattern.
    """
    
    def __init__(self, config: BenchmarkConfig) -> None:
        """
        Initialize the benchmark runner.
        
        Args:
            config: Benchmark configuration specifying queries and iterations
        """
        self.config = config
        self.results: list[BenchmarkResult] = []
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
    
    def _get_db_path(self) -> Path:
        """Get the path to the persistent database file."""
        return self.config.data_path / _get_db_filename(self.config.scale_factor)
    
    def _load_data(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Load TPC-H data from persistent storage into memory.
        
        Uses the ATTACH/COPY/DETACH pattern to load data.
        
        Args:
            conn: DuckDB connection (in-memory)
        """
        db_path = self._get_db_path()
        
        # Load data using ATTACH/COPY/DETACH pattern
        db_alias = "tpch_source"
        escaped_db_path = _escape_sql_string(str(db_path))
        conn.execute(f"ATTACH '{escaped_db_path}' AS {db_alias};")
        conn.execute(f"COPY FROM DATABASE {db_alias} TO memory;")
        conn.execute(f"DETACH {db_alias};")
    
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
    
    def _execute_query(
        self, conn: duckdb.DuckDBPyConnection, query_number: int, iteration: int
    ) -> BenchmarkResult:
        """
        Execute a single TPC-H query and measure its execution time.
        
        Args:
            conn: DuckDB connection
            query_number: TPC-H query number (1-22)
            iteration: Current iteration number
            
        Returns:
            BenchmarkResult containing execution metrics
        """
        # Get the TPC-H query using the extension's tpch_queries function
        try:
            query_result = conn.execute(
                f"SELECT query FROM tpch_queries() WHERE query_nr = {query_number};"
            ).fetchone()
            
            if query_result is None:
                return BenchmarkResult(
                    query_number=query_number,
                    iteration=iteration,
                    execution_time_ms=0.0,
                    row_count=0,
                    success=False,
                    error=f"Query {query_number} not found in tpch_queries()",
                )
            
            query_sql = query_result[0]
            
            # Execute and time the query
            start_time = time.perf_counter()
            result = conn.execute(query_sql)
            rows = result.fetchall()
            end_time = time.perf_counter()
            
            execution_time_ms = (end_time - start_time) * 1000
            
            return BenchmarkResult(
                query_number=query_number,
                iteration=iteration,
                execution_time_ms=execution_time_ms,
                row_count=len(rows),
                success=True,
            )
            
        except Exception as e:
            return BenchmarkResult(
                query_number=query_number,
                iteration=iteration,
                execution_time_ms=0.0,
                row_count=0,
                success=False,
                error=str(e),
            )
    
    def run(self) -> list[BenchmarkResult]:
        """
        Run all configured TPC-H queries.
        
        Uses a separate in-memory DuckDB connection that stays alive for
        the duration of the benchmark. Data is loaded using ATTACH/COPY/DETACH.
        
        Returns:
            List of BenchmarkResult for each query/iteration
            
        Raises:
            FileNotFoundError: If the database file doesn't exist
        """
        # Check if data exists before starting connection
        db_path = self._get_db_path()
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database file not found: {db_path}. "
                "Run data generation first."
            )
        
        self.results = []
        
        # Create a new in-memory connection for benchmarking
        # This is separate from any data generation connection
        conn = duckdb.connect(":memory:")
        
        try:
            # Install and load TPCH extension (needed for tpch_queries())
            self._install_and_load_tpch(conn)
            
            # Load data into memory
            self._load_data(conn)
            
            # Run each configured query for configured iterations
            for query_number in self.config.queries:
                for iteration in range(1, self.config.iterations + 1):
                    result = self._execute_query(conn, query_number, iteration)
                    self.results.append(result)
            
        finally:
            conn.close()
        
        return self.results
    
    def save_results(self, output_path: Optional[Path] = None) -> Path:
        """
        Save benchmark results to a JSON file.
        
        Args:
            output_path: Optional path override; uses config.output_path if not provided
            
        Returns:
            Path to the saved results file
            
        Raises:
            ValueError: If no results to save
        """
        if not self.results:
            raise ValueError("No benchmark results to save. Run the benchmark first.")
        
        # Determine output directory
        output_dir = output_path if output_path is not None else self.config.output_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename with timestamp and scale factor
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sf_str = _format_scale_factor(self.config.scale_factor)
        
        output_file = output_dir / f"benchmark_sf{sf_str}_{timestamp}.json"
        
        # Prepare output data
        output_data = {
            "config": {
                "scale_factor": self.config.scale_factor,
                "iterations": self.config.iterations,
                "queries": self.config.queries,
            },
            "timestamp": datetime.now().isoformat(),
            "results": [asdict(r) for r in self.results],
            "summary": self._compute_summary(),
        }
        
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        
        return output_file
    
    def _compute_summary(self) -> dict:
        """
        Compute summary statistics for the benchmark results.
        
        Returns:
            Dictionary containing summary statistics per query
        """
        summary = {}
        
        # Group results by query number
        for query_number in self.config.queries:
            query_results = [
                r for r in self.results 
                if r.query_number == query_number and r.success
            ]
            
            if query_results:
                times = [r.execution_time_ms for r in query_results]
                summary[f"query_{query_number}"] = {
                    "min_ms": min(times),
                    "max_ms": max(times),
                    "avg_ms": sum(times) / len(times),
                    "iterations": len(times),
                    "all_success": all(r.success for r in query_results),
                }
            else:
                summary[f"query_{query_number}"] = {
                    "min_ms": None,
                    "max_ms": None,
                    "avg_ms": None,
                    "iterations": 0,
                    "all_success": False,
                }
        
        return summary
