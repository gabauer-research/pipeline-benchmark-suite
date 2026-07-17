# Modular XML-Based Pipeline for Reproducible Scientific Data Processing

[![Python](https://img.shields.io/badge/Python-3.13%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey)](https://www.sqlite.org/)

This repository contains the source code accompanying the project paper:

> **A Modular XML-Based Pipeline for Reproducible Scientific Data Processing: Schema Validation and Provenance Tracking**  
> Christoph Gabauer, accompanying project repository, 2026

---

## Overview

The project implements a lightweight XML-based data processing pipeline for reproducible scientific measurement metadata processing. XML documents are validated against an XSD schema, selected metadata required for identification and provenance linkage are persisted in a SQLite database, and provenance metadata are recorded for each processing step.

The architecture follows a modular design with a strict separation of concerns. Validation, metadata extraction, SQLite persistence, provenance tracking, and orchestration are implemented as independent components.

Key properties:

- Strict XSD-based input validation
- Lightweight SQL-based provenance tracking without external workflow infrastructure
- Deterministic, single-process execution model
- Linear scalability across batch sizes of 100, 200, 500, and 1000 XML files
- Coefficient of determination of R² = 0.99997 for the fitted runtime model
- Constant peak memory footprint of approximately 28 MB in the reported evaluation
- FAIR-aligned support for interoperability and reusability

---

## Repository Structure

```text
├── src/
│   ├── pipeline.py          # Orchestration module controlling the end-to-end workflow
│   ├── validator.py         # Validation module for XSD-based schema validation
│   ├── extractor.py         # Extraction and persistence module for SQLite insertion
│   ├── provenance.py        # Provenance module for logging processing events
│   ├── db_init.py           # Database initialization script
│   ├── xml_generator.py     # Synthetic XML data generator for experiments
│   └── experiment_runner.py # Performance evaluation runner
├── schema/
│   └── schema.xsd           # XML Schema Definition used as the authoritative data contract
├── xml/                     # Sample XML files for functional validation
├── xml_pool/                # Generated XML pool for performance experiments
├── db/                      # SQLite database output, created at runtime
└── results/                 # Experiment result files, created at runtime
```

---

## Requirements

- Python 3.13+
- [lxml](https://lxml.de/) 6.0.2 for XML parsing and XSD validation
- [psutil](https://github.com/giampaolo/psutil) 7.2.2 for memory monitoring

Install dependencies from the repository root:

```bash
pip install -r requirements.txt
```

---

## Usage

The scripts use relative paths and are therefore expected to be executed from the `src/` directory.

### Functional Validation

The `xml/` directory contains five example documents: two valid XML files and three intentionally invalid XML files. These files demonstrate the validation and rejection behaviour of the pipeline.

Run the functional validation workflow:

```bash
cd src
python db_init.py
python pipeline.py
```

The pipeline validates each file in `../xml/` against `../schema/schema.xsd`.

Valid documents are persisted to SQLite and accompanied by provenance records. Invalid documents are rejected during schema validation and logged for auditability.

Example files:

```text
valid_01.xml
valid_02.xml
invalid_constraints.xml
invalid_missing_metadata.xml
invalid_structure.xml
```

---

## Performance Evaluation

The performance evaluation executes controlled batch experiments with generated XML files.

Run the full benchmark workflow:

```bash
cd src
python db_init.py
python xml_generator.py
python experiment_runner.py
```

The benchmark uses batch sizes of 100, 200, 500, and 1000 XML files with 20 repeated runs per configuration. The database is reset before each measured run to ensure identical initial conditions. A warm-up run is performed before timed execution for each batch size.

The following result files are written to `../results/`:

```text
experiment_results.txt
raw_runtime_measurements.csv
```

`experiment_results.txt` contains aggregated benchmark statistics, including mean runtime, median runtime, standard deviation, throughput, peak memory usage, per-file processing time, quartiles, and stage-level timings.

`raw_runtime_measurements.csv` contains the raw measurements used for reproducible boxplot generation and external analysis.

CSV columns:

```text
batch_size,run_id,runtime_ms,throughput_files_s,memory_peak_mb
```

---

## Database Schema

The SQLite database is created at runtime as `db/pipeline.db`. It contains two core tables: `metadata` and `provenance`.

### `metadata`

The `metadata` table stores validated measurement metadata records.

| Column | Type | Description |
|---|---|---|
| `id` | TEXT PK | Measurement identifier |
| `timestamp` | TEXT | Measurement timestamp |
| `geraet` | TEXT | Device identifier |
| `operator` | TEXT | Operator name |
| `parameter` | TEXT | Measured parameter |

### `provenance`

The `provenance` table records processing events and benchmark-related runtime metadata.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Technical primary key |
| `measurement_id` | TEXT FK | Reference to the corresponding metadata record |
| `step` | TEXT | Processing stage, such as validation, db_insert, or pipeline |
| `status` | TEXT | Processing outcome, such as success or error |
| `message` | TEXT | Event details or error message |
| `timestamp` | TEXT | Provenance event timestamp |
| `xml_file` | TEXT | Source XML filename |
| `xsd_schema` | TEXT | Referenced XSD schema |
| `schema_version` | TEXT | Schema version |
| `pipeline_version` | TEXT | Pipeline version |
| `processing_time_ms` | REAL | Total processing time |
| `memory_peak_mb` | REAL | Peak RSS memory |
| `validation_time_ms` | REAL | Validation stage duration |
| `extraction_time_ms` | REAL | Extraction stage duration |
| `persistence_time_ms` | REAL | Persistence stage duration |

---

## Provenance Queries

The provenance table can be queried using the SQLite CLI or any SQLite-compatible client, such as [DB Browser for SQLite](https://sqlitebrowser.org/).

### Processing history for a specific file

```sql
SELECT step, status, message, timestamp
FROM provenance
WHERE xml_file = 'valid_01.xml'
ORDER BY timestamp;
```

### All validation errors

```sql
SELECT xml_file, message, timestamp
FROM provenance
WHERE step = 'validation' AND status = 'error';
```

### Performance summary per pipeline version

```sql
SELECT pipeline_version,
       AVG(processing_time_ms) AS mean_ms,
       AVG(memory_peak_mb)     AS mean_mb
FROM provenance
WHERE step = 'pipeline' AND status = 'success'
GROUP BY pipeline_version;
```

---

## Reproducibility

To reproduce the performance evaluation, follow the steps described in [Performance Evaluation](#performance-evaluation).

The evaluation protocol uses:

- batch sizes of 100, 200, 500, and 1000 XML files
- 20 repeated runs per batch size
- database reset before each measured run
- one warm-up run per batch size
- high-resolution runtime measurement
- peak RSS memory measurement
- export of raw benchmark measurements to CSV

Experimental configuration used for the reported results:

| Component | Configuration |
|---|---|
| Operating system | Windows 11 Pro, Build 26200 |
| CPU | 8 cores, 4.20 GHz |
| RAM | 32 GB |
| Python | 3.13.11 |
| SQLite | 3.51.2 |

Runtime values may vary depending on hardware, operating-system state, and background processes. The raw CSV output is intended to support transparent inspection and reproducible visualization of the reported benchmark distribution.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
