# -*- coding: utf-8 -*-
"""
Provenance logger for the XML measurement data pipeline.
Writes structured provenance records to SQLite with stage-level
performance metrics for FAIR-compliant reproducibility.

License: MIT
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
        Write a provenance record to the SQLite database.

        Args:
            measurement_id: Identifier of the measurement being processed.
            step: Pipeline stage (e.g. 'validation', 'extraction', 'pipeline').
            status: Outcome status ('success' or 'error').
            message: Optional descriptive message.
            xml_file: Source XML filename.
            xsd_schema: XSD schema filename used for validation.
            schema_version: Version of the XSD schema.
            pipeline_version: Version of the pipeline software.
            processing_time_ms: Total processing time in milliseconds.
            memory_peak_mb: Peak RSS memory in megabytes.
            validation_time_ms: Time spent in validation stage.
            extraction_time_ms: Time spent in extraction stage.
            persistence_time_ms: Time spent in persistence stage.
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


# Convenience function for direct import
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
