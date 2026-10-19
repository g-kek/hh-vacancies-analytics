import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
from datetime import datetime
from time import perf_counter, sleep
from zoneinfo import ZoneInfo

import psutil

from hh_analytics.config import ROOT, settings
from hh_analytics.storage import bucket, s3_client, upload
from hh_analytics.utils import save_json, write_csv
from hh_analytics.validation import compare


def measured_run(command, log):
    start = perf_counter()
    peak, samples = 0, 0
    with log.open("w") as f:
        child = subprocess.Popen(
            command, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT
        )
        process = psutil.Process(child.pid)
        while child.poll() is None:
            total = 0
            try:
                tree = [process, *process.children(recursive=True)]
            except psutil.NoSuchProcess:
                tree = []
            for member in tree:
                try:
                    total += member.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            peak = max(peak, total)
            samples += 1
            sleep(0.01)
        elapsed = perf_counter() - start
    if child.returncode:
        raise RuntimeError(
            f"Код выхода {child.returncode}: {log}\n{log.read_text()[-3000:]}"
        )
    return {
        "wall_seconds": elapsed,
        "peak_tree_rss_mib": peak / 1024**2,
        "memory_samples": samples,
        "exit_code": child.returncode,
    }


def command(engine, cap, output):
    return [
        sys.executable,
        "-m",
        "hh_analytics.cli",
        "count",
        "--engine",
        engine,
        "--cap",
        str(cap),
        "--output",
        str(output),
        "--no-upload",
    ]


def run():
    cfg = settings()
    temporary = ROOT / ".runtime/benchmark"
    temporary.mkdir(parents=True, exist_ok=True)
    log_dir = ROOT / ".runtime/logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    environment = {
        "analysis_config": cfg,
        "input_sha256": s3_client().head_object(
            Bucket=bucket(), Key=cfg["object_key"]
        )["Metadata"]["sha256"],
        "run_at": datetime.now(ZoneInfo("Europe/Moscow")).isoformat(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": sys.version,
        "logical_cpus": os.cpu_count(),
        "host_memory_gib": psutil.virtual_memory().total / 1024**3,
        "sampling_interval_seconds": 0.01,
        "attempts": cfg["benchmark_attempts"],
        "cache_policy": (
            "Один прогрев на движок; кэш ОС и RustFS не "
            "сбрасывается; порядок ротируется"
        ),
        "memory_definition": (
            "Пик суммы RSS родителя и всех его живых "
            "потомков; общие страницы могут считаться "
            "повторно"
        ),
        "timing_definition": (
            "Новый процесс: импорты, S3, чтение, фильтр, "
            "расчёт, сериализация JSON; без загрузки "
            "результата S3"
        ),
        "limits": {
            "map_workers": 2,
            "chunk_rows": 2000,
            "duckdb_threads": 2,
            "duckdb_memory_limit": "1GB",
        },
    }
    for engine in ("baseline", "combined", "duckdb"):
        subprocess.run(
            command(
                engine, max(cfg["stack_limits"]), temporary / "warmup.json"
            ),
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
    (temporary / "warmup.json").unlink()
    metrics = []
    reference = {}
    engines = ["baseline", "combined", "duckdb"]
    for attempt in range(1, cfg["benchmark_attempts"] + 1):
        order = engines[attempt - 1 :] + engines[: attempt - 1]
        for cap in cfg["stack_limits"]:
            for engine in order:
                name = f"{engine}_k{cap}_run{attempt}"
                path = temporary / f"{name}.json"
                metrics_row = {
                    "engine": engine,
                    "cap": cap,
                    "attempt": attempt,
                    **measured_run(
                        command(engine, cap, path), log_dir / f"{name}.log"
                    ),
                }
                result = json.loads(path.read_text())
                if cap not in reference:
                    reference[cap] = result
                metrics_row["counts_sha256"] = compare(reference[cap], result)
                metrics_row["compute_seconds"] = result["compute_seconds"]
                metrics.append(metrics_row)
                print(f"{engine} K={cap} попытка {attempt}", flush=True)
                label = (
                    "duckdb" if engine == "duckdb" else f"mapreduce_{engine}"
                )
                destination = ROOT / f"results/counts/{label}_k{cap}.json"
                destination.parent.mkdir(exist_ok=True)
                shutil.copyfile(path, destination)
                path.unlink()
                save_json(
                    ROOT / "results/benchmark_environment.json", environment
                )
                write_csv(ROOT / "results/benchmark_runs.csv", metrics)
    summaries = []
    for cap in cfg["stack_limits"]:
        for engine in engines:
            rows = [
                r for r in metrics if r["cap"] == cap and r["engine"] == engine
            ]
            times = [r["wall_seconds"] for r in rows]
            memory = [r["peak_tree_rss_mib"] for r in rows]
            summaries.append(
                {
                    "engine": engine,
                    "cap": cap,
                    "attempts": len(rows),
                    "median_seconds": statistics.median(times),
                    "min_seconds": min(times),
                    "max_seconds": max(times),
                    "stdev_seconds": statistics.stdev(times),
                    "median_peak_rss_mib": statistics.median(memory),
                    "max_peak_rss_mib": max(memory),
                }
            )
    write_csv(ROOT / "results/benchmark_summary.csv", summaries)
    for name in (
        "benchmark_environment.json",
        "benchmark_runs.csv",
        "benchmark_summary.csv",
    ):
        path = ROOT / "results" / name
        upload(path, f"results/{cfg['variant']}/profiling/{path.name}")
    for path in (ROOT / "results/counts").glob("*.json"):
        upload(path, f"results/{cfg['variant']}/counts/{path.name}")
    temporary.rmdir()
