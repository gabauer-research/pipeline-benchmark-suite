# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 13:41:14 2026

@author: cgaba
"""

import os
import psutil
import time
import threading
from validator import XMLValidator
from extractor import MetadataExtractor
from provenance import log_provenance


class Pipeline:
    def __init__(self,
                 xml_dir="../xml/",
                 schema_path="../schema/schema.xsd",
                 db_path="../db/pipeline.db",
                 schema_version="1.0",
                 pipeline_version="0.9.1"):

        self.xml_dir = xml_dir
        self.schema_version = schema_version
        self.pipeline_version = pipeline_version
        self.db_path = db_path

        # Validator & Extractor bekommen Versionen mit
        self.validator = XMLValidator(
            schema_path=schema_path,
            schema_version=schema_version,
            pipeline_version=pipeline_version
        )

        self.extractor = MetadataExtractor(
            db_path=db_path,
            pipeline_version=pipeline_version
        )
        
        # Für Memory-Peak-Tracking
        self.peak_memory = 0
        self.monitoring = False

    def _monitor_memory(self):
        """Background-Thread der kontinuierlich den Peak Memory trackt."""
        process = psutil.Process()
        while self.monitoring:
            current_mem = process.memory_info().rss / (1024 * 1024)  # in MB
            self.peak_memory = max(self.peak_memory, current_mem)
            time.sleep(0.01)  # Sample alle 10ms

    def run(self, file_list=None):
        """
        Durchläuft alle XML-Dateien im xml/-Ordner
        und führt Validierung, Extraktion, DB-Insert und Logging durch.
        JETZT MIT ECHTEM PEAK MEMORY TRACKING.
        
        Args:
            file_list: Optional - Liste von Dateinamen. Wenn None, werden alle XMLs im xml_dir verarbeitet.
        """

        if file_list is None:
            xml_files = [f for f in os.listdir(self.xml_dir) if f.endswith(".xml")]
        else:
            xml_files = file_list

        print(f"Gefundene XML-Dateien: {len(xml_files)}")

        successful_runs = 0
        failed_runs = 0
        
        # Memory-Monitoring starten
        self.peak_memory = 0
        self.monitoring = True
        monitor_thread = threading.Thread(target=self._monitor_memory, daemon=True)
        monitor_thread.start()

        for filename in xml_files:
            xml_path = os.path.join(self.xml_dir, filename)

            # Performance-Tracking starten
            pipeline_start = time.perf_counter()
            
            metrics = {}

            # 1. Validierung (loggt selbst)
            val_start = time.perf_counter()
            validation_result = self.validator.validate(xml_path)
            metrics['validation_time_ms'] = (time.perf_counter() - val_start) * 1000

            if not validation_result["valid"]:
                failed_runs += 1
                continue

            # 2. Metadaten extrahieren (loggt selbst)
            ext_start = time.perf_counter()
            meta = self.extractor.extract_metadata(xml_path)
            metrics['extraction_time_ms'] = (time.perf_counter() - ext_start) * 1000

            if not meta["success"]:
                failed_runs += 1
                continue

            measurement_id = meta["data"]["id"]

            # 3. Metadaten in DB speichern (loggt selbst)
            pers_start = time.perf_counter()
            ok, err = self.extractor.insert_metadata(meta["data"], xml_path)
            metrics['persistence_time_ms'] = (time.perf_counter() - pers_start) * 1000

            if not ok:
                failed_runs += 1
                continue

            # 4. Gesamtmetriken berechnen
            metrics['processing_time_ms'] = (time.perf_counter() - pipeline_start) * 1000
            
            # Peak Memory wird vom Background-Thread getrackt
            metrics['memory_peak_mb'] = self.peak_memory

            # 5. Pipeline abgeschlossen MIT METRIKEN (nur dieser Schritt wird hier geloggt)
            log_provenance(
                measurement_id=measurement_id,
                step="pipeline",
                status="success",
                message="processing completed",
                xml_file=filename,
                pipeline_version=self.pipeline_version,
                processing_time_ms=metrics['processing_time_ms'],
                memory_peak_mb=metrics['memory_peak_mb'],
                validation_time_ms=metrics['validation_time_ms'],
                extraction_time_ms=metrics['extraction_time_ms'],
                persistence_time_ms=metrics['persistence_time_ms']
            )

            successful_runs += 1

        # Memory-Monitoring stoppen
        self.monitoring = False
        monitor_thread.join(timeout=0.1)

        return {
            "total": len(xml_files),
            "successful": successful_runs,
            "failed": failed_runs,
            "peak_memory_mb": self.peak_memory  # Echter Peak über gesamten Batch
        }


if __name__ == "__main__":
    pipeline = Pipeline()
    result = pipeline.run()
    print(f"\nErgebnis: {result['successful']}/{result['total']} erfolgreich verarbeitet")
    print(f"Peak Memory: {result['peak_memory_mb']:.2f}MB")