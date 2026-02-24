# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 13:38:27 2026

@author: cgaba
"""

import os
from lxml import etree
from provenance import log_provenance   # <-- wichtig: Provenance-Logger importieren


class XMLValidator:
    def __init__(self, schema_path="../schema/schema.xsd", schema_version="1.0", pipeline_version="0.9.1"):
        self.schema_path = schema_path
        self.schema_version = schema_version
        self.pipeline_version = pipeline_version

        # XSD laden
        with open(schema_path, "rb") as f:
            schema_doc = etree.XML(f.read())
            self.schema = etree.XMLSchema(schema_doc)

    def validate(self, xml_path):
        """
        Validiert eine XML-Datei gegen das XSD.
        Gibt ein Dict zurück:
        {
            "valid": True/False,
            "errors": [Liste von Fehlermeldungen]
        }
        """

        xml_filename = os.path.basename(xml_path)
        xsd_filename = os.path.basename(self.schema_path)

        try:
            xml_doc = etree.parse(xml_path)
            self.schema.assertValid(xml_doc)

            # --- Provenance: Erfolg ---
            log_provenance(
                measurement_id=xml_filename,
                step="validation",
                status="success",
                message="XML validated successfully",
                xml_file=xml_filename,
                xsd_schema=xsd_filename,
                schema_version=self.schema_version,
                pipeline_version=self.pipeline_version
            )

            return {
                "valid": True,
                "errors": []
            }

        except etree.DocumentInvalid as e:
            # Fehler extrahieren
            error_log = self.schema.error_log
            errors = [str(err) for err in error_log]

            # --- Provenance: Fehler ---
            log_provenance(
                measurement_id=xml_filename,
                step="validation",
                status="error",
                message="; ".join(errors),
                xml_file=xml_filename,
                xsd_schema=xsd_filename,
                schema_version=self.schema_version,
                pipeline_version=self.pipeline_version
            )

            return {
                "valid": False,
                "errors": errors
            }

        except Exception as e:
            # Allgemeine Fehler (z. B. Datei nicht gefunden)
            msg = f"Unexpected error: {str(e)}"

            log_provenance(
                measurement_id=xml_filename,
                step="validation",
                status="error",
                message=msg,
                xml_file=xml_filename,
                xsd_schema=xsd_filename,
                schema_version=self.schema_version,
                pipeline_version=self.pipeline_version
            )

            return {
                "valid": False,
                "errors": [msg]
            }


if __name__ == "__main__":
    validator = XMLValidator()
    test_file = "../xml/valid_01.xml"
    result = validator.validate(test_file)

    print(f"Validierung von {test_file}:")
    print(result)
