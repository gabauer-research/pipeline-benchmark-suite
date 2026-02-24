# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 13:39:40 2026

@author: cgaba
"""

import os
import sqlite3
from lxml import etree
from provenance import log_provenance   # <-- Provenance-Logger importieren


class MetadataExtractor:
    def __init__(self, db_path="../db/pipeline.db", pipeline_version="0.9.1"):
        self.db_path = db_path
        self.pipeline_version = pipeline_version

    def extract_metadata(self, xml_path):
        """
        Extrahiert Metadaten aus einer XML-Datei.
        Gibt ein Dict zurück:
        {
            "success": True/False,
            "data": {...} oder None,
            "error": Fehlermeldung oder None
        }
        """
        xml_filename = os.path.basename(xml_path)

        try:
            tree = etree.parse(xml_path)
            root = tree.getroot()

            # Metadaten auslesen
            metadata = root.find("metadata")
            if metadata is None:
                msg = "metadata section missing"

                log_provenance(
                    measurement_id=xml_filename,
                    step="metadata_extraction",
                    status="error",
                    message=msg,
                    xml_file=xml_filename,
                    pipeline_version=self.pipeline_version
                )

                return {
                    "success": False,
                    "data": None,
                    "error": msg
                }

            measurement_id = metadata.findtext("measurement_id")
            timestamp = metadata.findtext("timestamp")
            geraet = metadata.findtext("geraet")
            operator = metadata.findtext("operator")
            parameter = metadata.findtext("parameter")

            # Pflichtfelder prüfen
            if not measurement_id or not timestamp or not geraet:
                msg = "missing required metadata fields"

                log_provenance(
                    measurement_id=xml_filename,
                    step="metadata_extraction",
                    status="error",
                    message=msg,
                    xml_file=xml_filename,
                    pipeline_version=self.pipeline_version
                )

                return {
                    "success": False,
                    "data": None,
                    "error": msg
                }

            data = {
                "id": measurement_id,
                "timestamp": timestamp,
                "geraet": geraet,
                "operator": operator,
                "parameter": parameter
            }

            # --- Provenance: Erfolg ---
            log_provenance(
                measurement_id=measurement_id,
                step="metadata_extraction",
                status="success",
                message="metadata extracted",
                xml_file=xml_filename,
                pipeline_version=self.pipeline_version
            )

            return {
                "success": True,
                "data": data,
                "error": None
            }

        except Exception as e:
            msg = str(e)

            log_provenance(
                measurement_id=xml_filename,
                step="metadata_extraction",
                status="error",
                message=msg,
                xml_file=xml_filename,
                pipeline_version=self.pipeline_version
            )

            return {
                "success": False,
                "data": None,
                "error": msg
            }

    def insert_metadata(self, data, xml_path=None):
        """
        Speichert extrahierte Metadaten in SQLite.
        """
        xml_filename = os.path.basename(xml_path) if xml_path else None

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO metadata (id, timestamp, geraet, operator, parameter)
                VALUES (?, ?, ?, ?, ?)
            """, (
                data["id"],
                data["timestamp"],
                data["geraet"],
                data["operator"],
                data["parameter"]
            ))

            conn.commit()
            conn.close()

            # --- Provenance: Erfolg ---
            log_provenance(
                measurement_id=data["id"],
                step="db_insert",
                status="success",
                message="metadata stored",
                xml_file=xml_filename,
                pipeline_version=self.pipeline_version
            )

            return True, None

        except Exception as e:
            msg = str(e)

            log_provenance(
                measurement_id=data.get("id", "unknown"),
                step="db_insert",
                status="error",
                message=msg,
                xml_file=xml_filename,
                pipeline_version=self.pipeline_version
            )

            return False, msg


if __name__ == "__main__":
    extractor = MetadataExtractor()
    result = extractor.extract_metadata("../xml/valid_01.xml")
    print(result)

    if result["success"]:
        ok, err = extractor.insert_metadata(result["data"], "../xml/valid_01.xml")
        print("DB insert:", ok, err)
