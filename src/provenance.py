# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 13:40:29 2026

@author: cgaba
"""

import sqlite3
from datetime import datetime


class ProvenanceLogger:
    def __init__(self, db_path="../db/pipeline.db"):
        self.db_path = db_path

    def log_provenance(
        self,
        measurement_id,
        step,
        status,
        message=None,
        xml_file=None,
        xsd_schema=None,
        schema_version=None,
        pipeline_version="0.9.1",
        processing_time_ms=None,
        memory_peak_mb=None,
        validation_time_ms=None,
        extraction_time_ms=None,
        persistence_time_ms=None
    ):
        """
        Schreibt einen Provenance-Eintrag in die SQLite-Datenbank.
        Neue Felder:
            xml_file
            xsd_schema
            schema_version
            pipeline_version
            processing_time_ms
            memory_peak_mb
            validation_time_ms
            extraction_time_ms
            persistence_time_ms
        """

        timestamp = datetime.now().isoformat()

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO provenance (
                    measurement_id,
                    step,
                    status,
                    message,
                    timestamp,
                    xml_file,
                    xsd_schema,
                    schema_version,
                    pipeline_version,
                    processing_time_ms,
                    memory_peak_mb,
                    validation_time_ms,
                    extraction_time_ms,
                    persistence_time_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                measurement_id,
                step,
                status,
                message,
                timestamp,
                xml_file,
                xsd_schema,
                schema_version,
                pipeline_version,
                processing_time_ms,
                memory_peak_mb,
                validation_time_ms,
                extraction_time_ms,
                persistence_time_ms
            ))

            conn.commit()
            conn.close()

            return True, None

        except Exception as e:
            return False, str(e)


# Komfortfunktion für direkten Import
def log_provenance(*args, **kwargs):
    logger = ProvenanceLogger()
    return logger.log_provenance(*args, **kwargs)


if __name__ == "__main__":
    logger = ProvenanceLogger()
    ok, err = logger.log_provenance(
        measurement_id="M001",
        step="validation",
        status="success",
        message="XML validated successfully",
        xml_file="valid_01.xml",
        xsd_schema="schema.xsd",
        schema_version="1.0",
        pipeline_version="0.9.1"
    )
    print("Log:", ok, err)