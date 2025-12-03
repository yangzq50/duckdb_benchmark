# duckdb_benchmark

A clean, decoupled Python module for running in-memory DuckDB TPC-H benchmarks.

## Features

- **No hidden defaults**: All configuration must be explicitly provided
- **Configurable**: Supports TPC-H data persistence, benchmark output paths, query/iteration settings
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

## Usage

### CLI

Create a sample configuration file:

```bash
duckdb-benchmark init --output config.json
```

Generate TPC-H data (not yet implemented):

```bash
duckdb-benchmark generate --config config.json
```

Run benchmarks (not yet implemented):

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
    load_tpch_extension=True,
    in_memory=True,
)

# Data generation and benchmarking (placeholders for future implementation)
generator = DataGenerator(config)
benchmark = Benchmark(config)
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
  "load_tpch_extension": true,
  "in_memory": true
}
```

| Field | Description |
|-------|-------------|
| `scale_factor` | TPC-H scale factor (e.g., 1, 10, 100) |
| `data_path` | Directory for TPC-H data files |
| `output_path` | Directory for benchmark results |
| `iterations` | Number of iterations per query |
| `queries` | List of TPC-H queries to run (1-22) |
| `load_tpch_extension` | Whether to load DuckDB's TPC-H extension |
| `in_memory` | Run DuckDB in memory-only mode |

## Module Structure

```
duckdb_benchmark/
├── __init__.py        # Module exports
├── cli.py             # CLI entry point
├── config.py          # Configuration loader
├── data_generator.py  # TPC-H data generation (placeholder)
└── benchmark.py       # Benchmarking logic (placeholder)
```

## Development

Run tests:

```bash
pytest tests/
```

## License

MIT License
