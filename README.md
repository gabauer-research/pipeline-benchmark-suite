# Modular XML-Based Pipeline for Reproducible Scientific Data Processing

[![Python](https://img.shields.io/badge/Python-3.13%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey)](https://www.sqlite.org/)

This repository contains the source code accompanying the paper:

> **A Modular XML-Based Pipeline for Reproducible Scientific Data Processing: Schema Validation and Provenance Tracking**  
> Christoph Gabauer, *submitted to SN Computer Science*, 2026

---

## Overview

The pipeline processes scientific measurement data stored in XML format. It validates documents against an XSD schema, transforms hierarchical data into a normalised relational SQLite database, and records provenance metadata for every processing step. The architecture is designed around a strict separation of concerns: validation, persistence, provenance tracking, and orchestration are implemented as independent modules.

Key properties:
- 100% schema-based input validation via XSD
- Lightweight provenance tracking (SQL-based, no external framework required)
- Deterministic, single-process execution model
- Near-linear scalability (R² > 0.99 across batch sizes of 100–1000 files)
- Constant memory footprint (~34 MB independent of batch size)
- Alignment with FAIR principles (Interoperability, Reusability)

---

## Repository Structure

```
├── src/
│   ├── pipeline.py          # Orchestration module – controls end-to-end workflow
│   ├── validator.py         # Validation module – XSD-based schema validation (lxml)
│   ├── extractor.py         # Persistence module – metadata extraction and SQLite insert
│   ├── provenance.py        # Provenance module – logs processing events to DB
│   ├── db_init.py           # Database initialisation – creates schema and tables
│   ├── xml_generator.py     # Test data generator – creates synthetic valid XML files
│   └── experiment_runner.py # Performance evaluation – controlled batch experiments
├── schema/
│   └── schema.xsd           # XML Schema Definition (authoritative data contract)
├── xml/                     # Sample XML files (valid and invalid)
├── xml_pool/                # Generated XML pool for performance experiments
├── db/                      # SQLite database output (created at runtime)
└── results/                 # Experiment results output
```

---

## Requirements

- Python 3.13+
- [lxml](https://lxml.de/) – XML parsing and XSD validation
- [psutil](https://github.com/giampaolo/psutil) – memory monitoring

Install dependencies:

```bash
pip install lxml psutil
```

---

## Usage

### Functional Validation

The `xml/` directory contains five example documents (two valid, three intentionally
invalid) that demonstrate the validation behaviour of the pipeline.

**1. Initialise the database**
```bash
python src/db_init.py
```

**2. Run the pipeline**
```bash
python src/pipeline.py
```

The pipeline validates each file in `xml/` against `schema/schema.xsd`. Valid documents
(`valid_01.xml`, `valid_02.xml`) are persisted to SQLite and provenance records are
written for every processing event. Invalid documents (`invalid_constraints.xml`,
`invalid_missing_metadata.xml`, `invalid_structure.xml`) are rejected at the validation
stage and logged for auditability.

---

### Performance Evaluation

**1. Initialise the database**
```bash
python src/db_init.py
```

**2. Generate synthetic test data**
```bash
python src/xml_generator.py
```

Generates 1000 schema-conformant XML files in `xml_pool/`.

**3. Run the experiments**
```bash
python src/experiment_runner.py
```

Executes controlled experiments across batch sizes of 100, 200, 500, and 1000 files
with 20 repeated runs per configuration. Results are written to
`results/experiment_results.txt`.

---

## Database Schema

The SQLite database (`db/pipeline.db`) contains two tables:

**`metadata`** – stores validated measurement records:

| Column | Type | Description |
|---|---|---|
| `id` | TEXT PK | Measurement identifier |
| `timestamp` | TEXT | ISO 8601 timestamp |
| `geraet` | TEXT | Device identifier |
| `operator` | TEXT | Operator name |
| `parameter` | TEXT | Measured parameter |

**`provenance`** – records processing events for every file:

| Column | Type | Description |
|---|---|---|
| `measurement_id` | TEXT FK | Reference to metadata |
| `step` | TEXT | Processing stage (validation / db_insert / pipeline) |
| `status` | TEXT | Outcome (success / error) |
| `message` | TEXT | Details or error message |
| `timestamp` | TEXT | Event timestamp |
| `xml_file` | TEXT | Source filename |
| `xsd_schema` | TEXT | Schema filename |
| `schema_version` | TEXT | Schema version |
| `pipeline_version` | TEXT | Pipeline version |
| `processing_time_ms` | REAL | Total processing time |
| `memory_peak_mb` | REAL | Peak RSS memory |
| `validation_time_ms` | REAL | Validation stage duration |
| `extraction_time_ms` | REAL | Extraction stage duration |
| `persistence_time_ms` | REAL | Persistence stage duration |

---

## Provenance Queries (Examples)

```sql
-- Processing history for a specific file
SELECT step, status, message, timestamp
FROM provenance
WHERE xml_file = 'valid_01.xml'
ORDER BY timestamp;

-- All validation errors
SELECT xml_file, message, timestamp
FROM provenance
WHERE step = 'validation' AND status = 'error';

-- Performance summary per pipeline version
SELECT pipeline_version,
       AVG(processing_time_ms) AS mean_ms,
       AVG(memory_peak_mb)     AS mean_mb
FROM provenance
WHERE step = 'pipeline' AND status = 'success'
GROUP BY pipeline_version;
```

---

## Reproducibility

To reproduce the performance evaluation results reported in the paper, 
follow the steps under [Performance Evaluation](#performance-evaluation).

Each run resets the database to identical initial conditions before 
measurement. A warm-up run is performed prior to timed execution to 
minimise caching effects.

Experimental configuration used in the paper:
- OS: Windows 11 Pro (Build 26200)
- CPU: AMD Ryzen 7 7800X3D (8 cores, 4.20 GHz base)
- RAM: 32 GB
- Python: 3.13.11
- SQLite: 3.51.2

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
