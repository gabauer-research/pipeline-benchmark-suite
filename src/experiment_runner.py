# -*- coding: utf-8 -*-
"""
Experiment runner for the performance evaluation.

The script executes the XML processing pipeline under controlled conditions:
- batch sizes of 100, 200, 500, and 1000 XML files
- 20 repeated runs per batch size
- database reset before each measured run
- one warm-up run per batch size
- collection of runtime, throughput, memory, and stage-level metrics
- export of raw benchmark measurements as CSV for reproducible boxplots
"""

import csv
import os
import shutil
import sqlite3
import statistics
import time

from db_init import init_db
from pipeline import Pipeline


XML_SOURCE = "../xml_pool/"
XML_WORKDIR = "../xml_experiment/"
DB_PATH = "../db/pipeline.db"
RESULTS_FILE = "../results/experiment_results.txt"
RAW_RESULTS_FILE = "../results/raw_runtime_measurements.csv"

BATCH_SIZES = [100, 200, 500, 1000]
RUNS = 20


def reset_database():
    """Delete and re-initialize the SQLite database."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


def prepare_batch(batch_size):
    """
    Prepare a controlled batch of XML files.

    If the source pool contains fewer files than required, files are reused
    with unique target names.
    """
    if os.path.exists(XML_WORKDIR):
        shutil.rmtree(XML_WORKDIR)

    os.makedirs(XML_WORKDIR)

    all_files = sorted([
        filename for filename in os.listdir(XML_SOURCE)
        if filename.endswith(".xml")
    ])

    if not all_files:
        raise RuntimeError(f"No XML files found in {XML_SOURCE}.")

    selected = []

    for i in range(batch_size):
        source_name = all_files[i % len(all_files)]

        if i >= len(all_files):
            base, ext = os.path.splitext(source_name)
            target_name = f"{base}_copy{i}{ext}"
        else:
            target_name = source_name

        shutil.copy(
            os.path.join(XML_SOURCE, source_name),
            os.path.join(XML_WORKDIR, target_name)
        )

        selected.append(target_name)

    return selected


def get_stage_metrics_from_db():
    """
    Extract validation, extraction, and persistence timings from the database.

    Returns:
        A dictionary with mean, median, and standard deviation for each stage,
        or None if no stage-level data are available.
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

    validation_times = [row[0] for row in rows if row[0] is not None]
    extraction_times = [row[1] for row in rows if row[1] is not None]
    persistence_times = [row[2] for row in rows if row[2] is not None]

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


def get_quartiles(values):
    """
    Compute Q1 and Q3 for runtime documentation.

    The raw measurements exported to CSV remain the primary basis for boxplots.
    """
    if not values:
        return 0, 0

    if len(values) == 1:
        return values[0], values[0]

    quartiles = statistics.quantiles(values, n=4, method="inclusive")
    return quartiles[0], quartiles[2]


def save_results(results):
    """Save aggregated benchmark results as a human-readable text file."""
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)

    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        file.write("=" * 80 + "\n")
        file.write("PIPELINE PERFORMANCE EVALUATION RESULTS\n")
        file.write("=" * 80 + "\n\n")

        for batch_size, metrics in results.items():
            file.write(f"\nBatch Size: {batch_size} files\n")
            file.write("-" * 40 + "\n")
            file.write(f"Runs: {len(metrics['runtimes'])}\n")

            file.write(
                f"Mean Runtime: {metrics['mean_runtime'] * 1000:.2f}ms "
                f"(±{metrics['std_runtime'] * 1000:.2f}ms)\n"
            )
            file.write(f"Median Runtime: {metrics['median_runtime'] * 1000:.2f}ms\n")
            file.write(f"Min Runtime: {metrics['min_runtime'] * 1000:.2f}ms\n")
            file.write(f"Max Runtime: {metrics['max_runtime'] * 1000:.2f}ms\n")

            file.write(f"Runtime Q1: {metrics['q1_runtime'] * 1000:.2f}ms\n")
            file.write(f"Runtime Q3: {metrics['q3_runtime'] * 1000:.2f}ms\n")
            file.write(f"Runtime IQR: {metrics['iqr_runtime'] * 1000:.2f}ms\n")

            file.write("\nRaw Runtime Measurements [ms]:\n")
            for run_id, runtime in enumerate(metrics["runtimes"], start=1):
                file.write(f"  Run {run_id:02d}: {runtime * 1000:.2f}ms\n")

            file.write(f"\nMean Throughput: {metrics['mean_throughput']:.2f} files/s\n")
            file.write(f"Median Throughput: {metrics['median_throughput']:.2f} files/s\n")

            file.write(
                f"Mean Peak RSS: {metrics['mean_memory']:.2f}MB "
                f"(±{metrics['std_memory']:.2f}MB)\n"
            )
            file.write(f"Median Peak RSS: {metrics['median_memory']:.2f}MB\n")

            file.write(
                f"Mean Processing Time per File: "
                f"{metrics['mean_time_per_file']:.2f}ms\n"
            )
            file.write(
                f"Median Processing Time per File: "
                f"{metrics['median_time_per_file']:.2f}ms\n"
            )

            if metrics.get("stage_metrics"):
                stages = metrics["stage_metrics"]

                file.write("\nStage Breakdown:\n")
                file.write(
                    f"  Validation   - Mean: {stages['validation']['mean']:.2f}ms "
                    f"(±{stages['validation']['std']:.2f}ms), "
                    f"Median: {stages['validation']['median']:.2f}ms\n"
                )
                file.write(
                    f"  Extraction   - Mean: {stages['extraction']['mean']:.2f}ms "
                    f"(±{stages['extraction']['std']:.2f}ms), "
                    f"Median: {stages['extraction']['median']:.2f}ms\n"
                )
                file.write(
                    f"  Persistence  - Mean: {stages['persistence']['mean']:.2f}ms "
                    f"(±{stages['persistence']['std']:.2f}ms), "
                    f"Median: {stages['persistence']['median']:.2f}ms\n"
                )

            file.write("\n")


def save_raw_results_csv(results):
    """
    Save raw benchmark measurements as CSV.

    The CSV file is intended as the reproducible data source for boxplots,
    regression checks, and external inspection.
    """
    os.makedirs(os.path.dirname(RAW_RESULTS_FILE), exist_ok=True)

    with open(RAW_RESULTS_FILE, "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "batch_size",
            "run_id",
            "runtime_ms",
            "throughput_files_s",
            "memory_peak_mb"
        ])

        for batch_size, metrics in results.items():
            rows = zip(
                metrics["runtimes"],
                metrics["throughputs"],
                metrics["memory_peaks"]
            )

            for run_id, (runtime, throughput, memory_peak) in enumerate(rows, start=1):
                writer.writerow([
                    batch_size,
                    run_id,
                    f"{runtime * 1000:.2f}",
                    f"{throughput:.2f}",
                    f"{memory_peak:.2f}"
                ])


def run_experiments():
    """Execute the controlled benchmark experiments."""
    print("=" * 80)
    print("STARTING PERFORMANCE EVALUATION")
    print("=" * 80)

    results = {}

    for batch_size in BATCH_SIZES:
        print(f"\n{'=' * 80}")
        print(f"Batch Size: {batch_size} files")
        print(f"{'=' * 80}")

        runtimes = []
        throughputs = []
        memory_peaks = []

        for run in range(RUNS):
            print(f"Run {run + 1}/{RUNS}...", end=" ")

            reset_database()
            file_list = prepare_batch(batch_size)

            pipeline = Pipeline(xml_dir=XML_WORKDIR)

            if run == 0:
                print("(warm-up)...", end=" ")
                pipeline.run(file_list)
                reset_database()

            start = time.perf_counter()
            result = pipeline.run(file_list)
            runtime = time.perf_counter() - start

            successful = result["successful"]
            throughput = successful / runtime if runtime > 0 else 0

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

            print(
                f"✓ {runtime * 1000:.2f}ms "
                f"({throughput:.2f} files/s, {avg_memory:.2f}MB)"
            )

        mean_runtime = statistics.mean(runtimes)
        median_runtime = statistics.median(runtimes)
        q1_runtime, q3_runtime = get_quartiles(runtimes)
        iqr_runtime = q3_runtime - q1_runtime

        stage_metrics = get_stage_metrics_from_db()

        results[batch_size] = {
            "runtimes": runtimes,
            "throughputs": throughputs,
            "memory_peaks": memory_peaks,
            "mean_runtime": mean_runtime,
            "median_runtime": median_runtime,
            "std_runtime": statistics.stdev(runtimes) if len(runtimes) > 1 else 0,
            "min_runtime": min(runtimes),
            "max_runtime": max(runtimes),
            "q1_runtime": q1_runtime,
            "q3_runtime": q3_runtime,
            "iqr_runtime": iqr_runtime,
            "mean_throughput": statistics.mean(throughputs),
            "median_throughput": statistics.median(throughputs),
            "mean_memory": statistics.mean(memory_peaks),
            "median_memory": statistics.median(memory_peaks),
            "std_memory": statistics.stdev(memory_peaks) if len(memory_peaks) > 1 else 0,
            "mean_time_per_file": (mean_runtime / batch_size) * 1000,
            "median_time_per_file": (median_runtime / batch_size) * 1000,
            "stage_metrics": stage_metrics
        }

        print(f"\n{'=' * 80}")
        print(f"Summary for Batch Size {batch_size}:")
        print(
            f"  Mean Runtime: {results[batch_size]['mean_runtime'] * 1000:.2f}ms "
            f"(±{results[batch_size]['std_runtime'] * 1000:.2f}ms)"
        )
        print(f"  Median Runtime: {results[batch_size]['median_runtime'] * 1000:.2f}ms")
        print(f"  Runtime Q1/Q3: {q1_runtime * 1000:.2f}ms / {q3_runtime * 1000:.2f}ms")
        print(f"  Mean Throughput: {results[batch_size]['mean_throughput']:.2f} files/s")
        print(f"  Mean Peak RSS: {results[batch_size]['mean_memory']:.2f}MB")
        print(f"  Mean Time per File: {results[batch_size]['mean_time_per_file']:.2f}ms")

        if stage_metrics:
            print("  Stage Times:")
            print(f"    Validation:  {stage_metrics['validation']['mean']:.2f}ms")
            print(f"    Extraction:  {stage_metrics['extraction']['mean']:.2f}ms")
            print(f"    Persistence: {stage_metrics['persistence']['mean']:.2f}ms")

        print(f"{'=' * 80}")

    save_results(results)
    save_raw_results_csv(results)

    print(f"\n{'=' * 80}")
    print("EVALUATION COMPLETE")
    print(f"Results saved to: {RESULTS_FILE}")
    print(f"Raw measurements saved to: {RAW_RESULTS_FILE}")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    run_experiments()
