# -*- coding: utf-8 -*-
"""
Database initialization for the XML measurement data pipeline.
Creates metadata and provenance tables for FAIR-compliant provenance logging.

License: MIT
"""

import sqlite3

def init_db():
    """Initialize SQLite database and create metadata and provenance tables if they do not exist."""
    conn = sqlite3.connect("../db/pipeline.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS metadata (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        geraet TEXT NOT NULL,
        operator TEXT,
        parameter TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS provenance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        measurement_id TEXT,
        step TEXT NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        timestamp TEXT NOT NULL,
        xml_file TEXT,
        xsd_schema TEXT,
        schema_version TEXT,
        pipeline_version TEXT,
        processing_time_ms REAL,
        memory_peak_mb REAL,
        validation_time_ms REAL,
        extraction_time_ms REAL,
        persistence_time_ms REAL,
        FOREIGN KEY (measurement_id) REFERENCES metadata(id)
    );
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":

    init_db()
