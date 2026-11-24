"""Run the homework 3 MapReduce calculation as Kubernetes jobs."""

import argparse
import csv
import json
import os
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep

from hh_analytics.config import ROOT, settings
from hh_analytics.processing.mapreduce import chunks, map_chunk, run
from hh_analytics.storage import bucket, s3_client, stream_csv, upload
from hh_analytics.utils import save_json
from hh_analytics.validation import compare

MEMORY_PEAK = Path("/sys/fs/cgroup/memory.peak")
MEMORY_PEAK_V1 = Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
SHARD_ROWS = 60_000


def log(**fields):
    print(json.dumps(fields, ensure_ascii=False), flush=True)


def read_json(key):
    response = s3_client().get_object(Bucket=bucket(), Key=key)
    with response["Body"] as body:
        return json.load(body)


def write_json(key, value):
    with TemporaryDirectory() as directory:
        path = Path(directory) / "result.json"
        save_json(path, value)
        upload(path, key)


def top_pairs(counts):
    pairs = (row for row in counts if len(row["skills"]) == 2)
    return sorted(pairs, key=lambda row: (-row["count"], row["skills"]))[:10]


def count_rows(_config, cap, args):
    log(stage="count_start", cap=cap, hold_seconds=args.hold_seconds)
    sleep(args.hold_seconds)
    with TemporaryDirectory() as directory:
        result = run(
            cap,
            workers=2,
            output=Path(directory) / "counts.json",
            publish=False,
        )
        peak_file = MEMORY_PEAK if MEMORY_PEAK.exists() else MEMORY_PEAK_V1
        log(
            stage="count_complete",
            rows=result["stats"]["rows_read"],
            pairs=result["stats"]["emitted_2"],
            top10=top_pairs(result["counts"]),
            memory_peak_bytes=int(peak_file.read_text()),
        )


def split(config, _cap, args):
    partitions = []
    with TemporaryDirectory() as directory:
        path = Path(directory) / "shard.csv"
        rows = stream_csv(config["object_key"])

        for index, part in enumerate(chunks(rows, SHARD_ROWS)):
            with path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(part[0]))
                writer.writeheader()
                writer.writerows(part)

            key = f"{args.prefix}/input/{index}.csv"
            info = {"index": index, "rows": len(part), **upload(path, key)}
            partitions.append(info)
            log(stage="split", **info)

    if not partitions:
        raise ValueError("Исходный CSV не содержит данных")

    write_json(f"{args.prefix}/manifest.json", partitions)


def pack_counts(counts):
    return [
        {"skills": list(key[1:]), "count": value}
        for key, value in sorted(counts.items())
    ]


def map_partition(config, cap, args):
    index = int(os.environ["JOB_COMPLETION_INDEX"])
    info = read_json(f"{args.prefix}/manifest.json")[index]
    log(stage="map_start", index=index, input_key=info["key"])

    counts, stats = map_chunk(
        stream_csv(info["key"]),
        config["title_regex"],
        cap,
        role=config.get("role"),
        c_pattern=config.get("c_title_regex"),
        exclude=config.get("c_exclude_regex"),
    )
    if stats["rows_read"] != info["rows"]:
        raise ValueError(f"Число строк части {index} не совпало")

    result = {
        "source_sha256": info["sha256"],
        "cap": cap,
        "stats": stats,
        "counts": pack_counts(counts),
    }
    key = f"{args.prefix}/map/{index}.json"
    write_json(key, result)
    log(stage="map_complete", index=index, key=key, **stats)


def reduce_counts(config, cap, args):
    partitions = read_json(f"{args.prefix}/manifest.json")
    counts = Counter()
    stats = Counter()

    for info in partitions:
        index = info["index"]
        result = read_json(f"{args.prefix}/map/{index}.json")
        if (
            result["source_sha256"] != info["sha256"]
            or result["cap"] != cap
            or result["stats"]["rows_read"] != info["rows"]
        ):
            raise ValueError(f"Результат части {index} не соответствует входу")

        stats.update(result["stats"])
        for row in result["counts"]:
            skills = row["skills"]
            counts[(len(skills), *skills)] += row["count"]

    entries = pack_counts(counts)
    result = {
        "cap": cap,
        "variant": config["variant"],
        "stats": dict(stats),
        "counts": entries,
    }
    key = f"{args.prefix}/result.json"
    write_json(key, result)
    log(stage="reduce_complete", key=key, shards=len(partitions), **stats)


def verify(config, cap, args):
    old_path = ROOT / "results/counts" / f"mapreduce_combined_k{cap}.json"
    expected = json.loads(old_path.read_text(encoding="utf-8"))
    actual = read_json(f"{args.prefix}/result.json")
    if actual["stats"]["rows_read"] != expected["stats"]["rows_read"]:
        raise AssertionError("Число входных строк отличается от ДЗ №3")

    digest = compare(expected, actual)
    pairs = top_pairs(actual["counts"])
    if pairs != top_pairs(expected["counts"]):
        raise AssertionError("Топ-10 отличается от ДЗ №3")

    evidence = {
        "variant": config["variant"],
        "cap": cap,
        "all_counts_equal": True,
        "top10_equal": True,
        "sha256": digest,
        "unique_combinations": len(actual["counts"]),
        "stats": actual["stats"],
        "top10": pairs,
    }
    save_json(args.output, evidence)
    log(
        stage="verify_complete",
        sha256=digest,
        rows=actual["stats"]["rows_read"],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="stage", required=True)

    for name, handler in (
        ("count", count_rows),
        ("split", split),
        ("map", map_partition),
        ("reduce", reduce_counts),
        ("verify", verify),
    ):
        action = actions.add_parser(name)
        action.set_defaults(handler=handler)

        if name == "count":
            action.add_argument("--hold-seconds", type=int, default=0)
        else:
            action.add_argument("--prefix", default="hw5/06-c-cpp/k20")

        if name == "verify":
            action.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()
    config = settings()
    args.handler(config, max(config["stack_limits"]), args)


if __name__ == "__main__":
    main()
