# -*- coding: utf-8 -*-
"""
Performance evaluation runner for the XML measurement data pipeline.
Tests the pipeline under controlled conditions across varying batch sizes
with repeated runs, database resets, and stage-level metric collection.

License: MIT
"""

import os
import shutil
import time
import sqlite3
import statistics
from pipeline import Pipeline
from db_init import init_db

# Configuration
XML_SOURCE = "../xml_pool/"           # Pool of all available XML files
XML_WORKDIR = "../xml_experiment/"  # # Temporary working directory for experiments
DB_PATH = "../db/pipeline.db"
RESULTS_FILE = "../results/experiment_results.txt"

BATCH_SIZES = [100, 200, 500, 1000]    # Number of XML files per experiment
RUNS = 20                               # Repetitions per batch size


def reset_database():
    """Delete and reinitialize the pipeline database."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


def prepare_batch(batch_size):
    """
    Prepare a controlled XML dataset of the given size.

    Args:
        batch_size: Number of XML files to prepare.

    Returns:
        List of filenames in the working directory.
    """
    # Clear and recreate working directory
    if os.path.exists(XML_WORKDIR):
        shutil.rmtree(XML_WORKDIR)
    os.makedirs(XML_WORKDIR)

    # Load all available XML files
    all_files = sorted([f for f in os.listdir(XML_SOURCE) if f.endswith(".xml")])
    
    # Repeat files if pool is smaller than batch size
    selected = []
    for i in range(batch_size):
        file_to_copy = all_files[i % len(all_files)]
        
        # Generate unique filename for duplicates
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
    Extract per-stage timing metrics from the provenance table.

    Returns:
        Dict with mean, median, and std for validation, extraction, and persistence stages,
        or None if no records are found.
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
    """Write experiment results to the results file."""
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
    """Run controlled performance experiments across all configured batch sizes."""
    
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
            
            # Ensure identical starting conditions for each run
            reset_database()
            file_list = prepare_batch(batch_size)
            
            # Pipeline mit temporärem Arbeitsverzeichnis
            pipeline = Pipeline(xml_dir=XML_WORKDIR)
            
            # Warmup run (first iteration only)
            if run == 0:
                print("(Warmup)...", end=" ")
                pipeline.run(file_list)
                reset_database()  # Reset after warmup
            
            # Actual measurement run
            start = time.perf_counter()
            result = pipeline.run(file_list)
            runtime = time.perf_counter() - start
            
            # Compute throughput
            successful = result['successful']
            throughput = successful / runtime if runtime > 0 else 0
            
            # Read peak memory from provenance table
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
        
        # Compute summary statistics
        mean_runtime = statistics.mean(runtimes)
        median_runtime = statistics.median(runtimes)
        
        # Collect stage metrics from last run before final reset
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
    
    # Save results to file
    save_results(results)
    
    print(f"\n{'='*80}")
    print(f"EVALUATION COMPLETE")
    print(f"Results saved to: {RESULTS_FILE}")
    print(f"{'='*80}\n")


if __name__ == "__main__":

    run_experiments()
