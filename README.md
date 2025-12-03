# duckdb_benchmark

A clean, decoupled Python module for running in-memory DuckDB TPC-H benchmarks.

## Features

- **No hidden defaults**: All configuration must be explicitly provided
- **Configurable**: Supports TPC-H data persistence, benchmark output paths, query/iteration settings, and custom extension paths
- **In-memory only**: DuckDB always runs in memory; data is persisted to disk using ATTACH/COPY/DETACH
- **Extensible**: Clear module structure ready for future features
- **Portable**: Can be copied into other projects as a standalone module

## Installation

```bash
pip install -e .
```

For development:

```bash
pip install -e ".[dev]"
```

**Note**: Requires DuckDB. Install with:

```bash
pip install duckdb
```

## Usage

### CLI

Create a sample configuration file:

```bash
duckdb-benchmark init --output config.json
```

Generate TPC-H data:

```bash
duckdb-benchmark generate --config config.json
```

Run benchmarks:

```bash
duckdb-benchmark run --config config.json
```

### Python API

```python
from duckdb_benchmark import BenchmarkConfig, DataGenerator, Benchmark

# Create configuration explicitly - no hidden defaults
config = BenchmarkConfig(
    scale_factor=1.0,
    data_path="./data",
    output_path="./results",
    iterations=3,
    queries=[1, 5, 10, 15, 20],
    tpch_extension_path=None,  # Uses bundled extension, or provide path
)

# Generate TPC-H data (persisted to disk)
generator = DataGenerator(config)
if not generator.data_exists():
    db_path = generator.generate()
    print(f"Data generated at {db_path}")

# Run benchmarks (uses separate connection, data loaded into memory)
benchmark = Benchmark(config)
results = benchmark.run()

# Save results
output_file = benchmark.save_results()
print(f"Results saved to {output_file}")
```

## Configuration

Configuration file format (JSON):

```json
{
  "scale_factor": 1.0,
  "data_path": "./data",
  "output_path": "./results",
  "iterations": 3,
  "queries": [1, 2, 3, 4, 5],
  "tpch_extension_path": null
}
```

| Field | Description |
|-------|-------------|
| `scale_factor` | TPC-H scale factor (e.g., 0.01, 1, 10, 100) |
| `data_path` | Directory for TPC-H data files (database files with sf in name) |
| `output_path` | Directory for benchmark results |
| `iterations` | Number of iterations per query |
| `queries` | List of TPC-H queries to run (1-22) |
| `tpch_extension_path` | Optional path to custom TPC-H extension file; null uses bundled |

## Architecture

### Data Generation

- DuckDB runs in-memory only
- TPC-H extension is installed and loaded
- Data is generated using `CALL dbgen(sf=N)`
- Data is persisted using the ATTACH/COPY/DETACH pattern:
  ```sql
  ATTACH 'path/to/tpch_sfN.db' AS tpch_persist;
  COPY FROM DATABASE memory TO tpch_persist;
  DETACH tpch_persist;
  ```

### Benchmark Execution

- Each benchmark run uses a separate DuckDB connection
- Connection stays alive for the duration of one benchmark at one scale factor
- Data is loaded into memory using:
  ```sql
  ATTACH 'path/to/tpch_sfN.db' AS tpch_source;
  COPY FROM DATABASE tpch_source TO memory;
  DETACH tpch_source;
  ```
- Queries are retrieved using `tpch_queries()` function
- Execution time is measured for each query

## Module Structure

```
duckdb_benchmark/
├── __init__.py        # Module exports
├── cli.py             # CLI entry point
├── config.py          # Configuration loader
├── data_generator.py  # TPC-H data generation
└── benchmark.py       # Benchmarking logic
```

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```

### Linting and Type Checking

This project uses [ruff](https://github.com/astral-sh/ruff) for linting and [mypy](https://mypy.readthedocs.io/) for type checking.

Run the linter:

```bash
ruff check .
```

Run the type checker:

```bash
mypy duckdb_benchmark tests
```

Both linting and type checking are enforced in CI and must pass for PRs to be merged.

## License

MIT License
