# -*- coding: utf-8 -*-
"""
Experiment Runner für journal-konforme Performance-Evaluation

Testet die Pipeline mit kontrollierten Bedingungen:
- Verschiedene Batchgrößen (100, 200, 500, 1000 XMLs)
- 20 Wiederholungen pro Batchgröße
- DB-Reset vor jedem Run
- Warmup-Run
- Stage-Metriken (Validation/Extraction/Persistence)

@author: cgaba
"""

import os
import shutil
import time
import sqlite3
import statistics
from pipeline import Pipeline
from db_init import init_db

# Konfiguration
XML_SOURCE = "../xml_pool/"           # Pool mit allen verfügbaren XMLs
XML_WORKDIR = "../xml_experiment/"  # Temporäres Arbeitsverzeichnis für Experimente
DB_PATH = "../db/pipeline.db"
RESULTS_FILE = "../results/experiment_results.txt"

BATCH_SIZES = [100, 200, 500, 1000]    # Anzahl XMLs pro Experiment
RUNS = 20                               # Wiederholungen pro Batchgröße


def reset_database():
    """Löscht und initialisiert die Datenbank neu."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


def prepare_batch(batch_size):
    """
    Bereitet ein kontrolliertes Dataset vor.
    
    Args:
        batch_size: Anzahl der XML-Dateien
    
    Returns:
        Liste der Dateinamen
    """
    # Workdir leeren und neu erstellen
    if os.path.exists(XML_WORKDIR):
        shutil.rmtree(XML_WORKDIR)
    os.makedirs(XML_WORKDIR)

    # Alle verfügbaren XMLs laden
    all_files = sorted([f for f in os.listdir(XML_SOURCE) if f.endswith(".xml")])
    
    # Falls nicht genug Dateien vorhanden: wiederholen
    selected = []
    for i in range(batch_size):
        file_to_copy = all_files[i % len(all_files)]
        
        # Eindeutigen Namen generieren falls Duplikat
        if i >= len(all_files):
            base, ext = os.path.splitext(file_to_copy)
            target_name = f"{base}_copy{i}{ext}"
        else:
            target_name = file_to_copy
        
        shutil.copy(
            os.path.join(XML_SOURCE, file_to_copy),
            os.path.join(XML_WORKDIR, target_name)
        )
        selected.append(target_name)

    return selected


def get_stage_metrics_from_db():
    """
    Extrahiert Stage-Metriken (Validation/Extraction/Persistence) aus der DB.
    
    Returns:
        dict mit mean/median/std für jede Stage
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            validation_time_ms,
            extraction_time_ms,
            persistence_time_ms
        FROM provenance 
        WHERE step = 'pipeline' AND status = 'success'
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return None
    
    # Transponieren: Liste von Tupeln → Listen pro Stage
    validation_times = [r[0] for r in rows if r[0] is not None]
    extraction_times = [r[1] for r in rows if r[1] is not None]
    persistence_times = [r[2] for r in rows if r[2] is not None]
    
    return {
        "validation": {
            "mean": statistics.mean(validation_times) if validation_times else 0,
            "median": statistics.median(validation_times) if validation_times else 0,
            "std": statistics.stdev(validation_times) if len(validation_times) > 1 else 0
        },
        "extraction": {
            "mean": statistics.mean(extraction_times) if extraction_times else 0,
            "median": statistics.median(extraction_times) if extraction_times else 0,
            "std": statistics.stdev(extraction_times) if len(extraction_times) > 1 else 0
        },
        "persistence": {
            "mean": statistics.mean(persistence_times) if persistence_times else 0,
            "median": statistics.median(persistence_times) if persistence_times else 0,
            "std": statistics.stdev(persistence_times) if len(persistence_times) > 1 else 0
        }
    }


def save_results(results):
    """Speichert Experiment-Ergebnisse in Datei."""
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("PIPELINE PERFORMANCE EVALUATION RESULTS\n")
        f.write("=" * 80 + "\n\n")
        
        for batch_size, metrics in results.items():
            f.write(f"\nBatch Size: {batch_size} files\n")
            f.write("-" * 40 + "\n")
            f.write(f"Runs: {len(metrics['runtimes'])}\n")
            
            # Gesamtzeiten
            f.write(f"Mean Runtime: {metrics['mean_runtime']*1000:.2f}ms (±{metrics['std_runtime']*1000:.2f}ms)\n")
            f.write(f"Median Runtime: {metrics['median_runtime']*1000:.2f}ms\n")
            f.write(f"Min Runtime: {metrics['min_runtime']*1000:.2f}ms\n")
            f.write(f"Max Runtime: {metrics['max_runtime']*1000:.2f}ms\n")
            
            # Throughput
            f.write(f"Mean Throughput: {metrics['mean_throughput']:.2f} files/s\n")
            f.write(f"Median Throughput: {metrics['median_throughput']:.2f} files/s\n")
            
            # Memory
            f.write(f"Mean Peak RSS: {metrics['mean_memory']:.2f}MB (±{metrics['std_memory']:.2f}MB)\n")
            f.write(f"Median Peak RSS: {metrics['median_memory']:.2f}MB\n")
            
            # Pro-File Zeit
            f.write(f"Mean Processing Time per File: {metrics['mean_time_per_file']:.2f}ms\n")
            f.write(f"Median Processing Time per File: {metrics['median_time_per_file']:.2f}ms\n")
            
            # STAGE-METRIKEN (NEU!)
            if 'stage_metrics' in metrics and metrics['stage_metrics']:
                f.write("\nStage Breakdown:\n")
                stages = metrics['stage_metrics']
                
                f.write(f"  Validation   - Mean: {stages['validation']['mean']:.2f}ms "
                       f"(±{stages['validation']['std']:.2f}ms), "
                       f"Median: {stages['validation']['median']:.2f}ms\n")
                
                f.write(f"  Extraction   - Mean: {stages['extraction']['mean']:.2f}ms "
                       f"(±{stages['extraction']['std']:.2f}ms), "
                       f"Median: {stages['extraction']['median']:.2f}ms\n")
                
                f.write(f"  Persistence  - Mean: {stages['persistence']['mean']:.2f}ms "
                       f"(±{stages['persistence']['std']:.2f}ms), "
                       f"Median: {stages['persistence']['median']:.2f}ms\n")
            
            f.write("\n")


def run_experiments():
    """Führt kontrollierte Experimente durch."""
    
    print("=" * 80)
    print("STARTING PERFORMANCE EVALUATION")
    print("=" * 80)
    
    results = {}
    
    for batch_size in BATCH_SIZES:
        print(f"\n{'='*80}")
        print(f"Batch Size: {batch_size} files")
        print(f"{'='*80}")
        
        runtimes = []
        throughputs = []
        memory_peaks = []
        
        for run in range(RUNS):
            print(f"Run {run + 1}/{RUNS}...", end=" ")
            
            # WICHTIG: Gleiche Startbedingungen
            reset_database()
            file_list = prepare_batch(batch_size)
            
            # Pipeline mit temporärem Arbeitsverzeichnis
            pipeline = Pipeline(xml_dir=XML_WORKDIR)
            
            # Warmup (nur beim ersten Run)
            if run == 0:
                print("(Warmup)...", end=" ")
                pipeline.run(file_list)
                reset_database()  # Nach Warmup auch resetten
            
            # Eigentlicher Messrun
            start = time.perf_counter()
            result = pipeline.run(file_list)
            runtime = time.perf_counter() - start
            
            # Metriken berechnen
            successful = result['successful']
            throughput = successful / runtime if runtime > 0 else 0
            
            # Peak Memory aus DB auslesen
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AVG(memory_peak_mb) 
                FROM provenance 
                WHERE step = 'pipeline' AND status = 'success'
            """)
            row = cursor.fetchone()
            avg_memory = row[0] if row and row[0] is not None else 0.0
            conn.close()
            
            runtimes.append(runtime)
            throughputs.append(throughput)
            memory_peaks.append(avg_memory)
            
            print(f"✓ {runtime*1000:.2f}ms ({throughput:.2f} files/s, {avg_memory:.2f}MB)")
        
        # Statistiken berechnen (MIT MEDIAN!)
        mean_runtime = statistics.mean(runtimes)
        median_runtime = statistics.median(runtimes)
        
        # WICHTIG: Stage-Metriken aus der DB holen (VOR dem finalen Reset!)
        # Wir nehmen die Daten vom letzten Run als repräsentativ
        stage_metrics = get_stage_metrics_from_db()
        
        results[batch_size] = {
            "runtimes": runtimes,
            "mean_runtime": mean_runtime,
            "median_runtime": median_runtime,
            "std_runtime": statistics.stdev(runtimes) if len(runtimes) > 1 else 0,
            "min_runtime": min(runtimes),
            "max_runtime": max(runtimes),
            "mean_throughput": statistics.mean(throughputs),
            "median_throughput": statistics.median(throughputs),
            "mean_memory": statistics.mean(memory_peaks),
            "median_memory": statistics.median(memory_peaks),
            "std_memory": statistics.stdev(memory_peaks) if len(memory_peaks) > 1 else 0,
            "mean_time_per_file": (mean_runtime / batch_size) * 1000,
            "median_time_per_file": (median_runtime / batch_size) * 1000,
            "stage_metrics": stage_metrics  # NEU!
        }
        
        print(f"\n{'='*80}")
        print(f"Summary for Batch Size {batch_size}:")
        print(f"  Mean Runtime: {results[batch_size]['mean_runtime']*1000:.2f}ms (±{results[batch_size]['std_runtime']*1000:.2f}ms)")
        print(f"  Median Runtime: {results[batch_size]['median_runtime']*1000:.2f}ms")
        print(f"  Mean Throughput: {results[batch_size]['mean_throughput']:.2f} files/s")
        print(f"  Mean Peak RSS: {results[batch_size]['mean_memory']:.2f}MB")
        print(f"  Mean Time per File: {results[batch_size]['mean_time_per_file']:.2f}ms")
        
        if stage_metrics:
            print(f"  Stage Times:")
            print(f"    Validation:  {stage_metrics['validation']['mean']:.2f}ms")
            print(f"    Extraction:  {stage_metrics['extraction']['mean']:.2f}ms")
            print(f"    Persistence: {stage_metrics['persistence']['mean']:.2f}ms")
        
        print(f"{'='*80}")
    
    # Ergebnisse speichern
    save_results(results)
    
    print(f"\n{'='*80}")
    print(f"EVALUATION COMPLETE")
    print(f"Results saved to: {RESULTS_FILE}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_experiments()