import itertools
import math
import multiprocessing
import re
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from time import perf_counter

from hh_analytics.config import ROOT, settings
from hh_analytics.storage import stream_csv, upload
from hh_analytics.utils import save_json


def normalize_skills(value):
    return sorted(
        {
            s.strip(" \t\r\n").lower()
            for s in value.split(";")
            if s.strip(" \t\r\n")
        }
    )


def title_matches(name, pattern, c_pattern=None, exclude=None):
    cpp = re.search(pattern, name.lower()) is not None

    c = bool(
        c_pattern
        and re.search(c_pattern, name)
        and not (exclude and re.search(exclude, name.lower()))
    )

    return cpp or c


def map_chunk(
    rows,
    pattern,
    cap,
    combine=True,
    fail=False,
    role=None,
    c_pattern=None,
    exclude=None,
):
    if fail:
        raise RuntimeError(
            "Контрольный отказ маппера до передачи результата редьюсеру"
        )

    counts = Counter()
    emissions = []
    stats = Counter()

    for row in rows:
        stats["rows_read"] += 1

        if role and row.get("role") != role:
            continue

        if not title_matches(row["name"], pattern, c_pattern, exclude):
            continue

        stats["vacancies"] += 1
        all_skills = normalize_skills(row["key_skills"])
        skills = all_skills[:cap] if cap else all_skills
        stats["vacancies_with_skills"] += bool(skills)
        stats["truncated_vacancies"] += bool(cap and len(all_skills) > cap)

        for order in (1, 2, 3):
            stats[f"emitted_{order}"] += math.comb(len(skills), order)
            keys = ((order, *c) for c in itertools.combinations(skills, order))

            if combine:
                counts.update(keys)
            else:
                emissions.extend(keys)

    if not combine:
        return emissions, dict(stats)

    return counts, dict(stats)


def chunks(rows, size):
    iterator = iter(rows)

    while part := list(itertools.islice(iterator, size)):
        yield part


def run(
    cap,
    mode="combined",
    workers=2,
    chunk_size=2000,
    inject_failure=False,
    output=None,
    publish=True,
):
    cfg = settings()
    counts, stats = Counter(), Counter()
    events = []
    start = perf_counter()
    rows = stream_csv(cfg["object_key"])
    batches = enumerate(chunks(rows, chunk_size))
    source_rows_read = 0
    context = multiprocessing.get_context("spawn")

    with ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool:
        pending = {}

        def submit(task_id, part, attempt=0):
            if mode == "combined" and attempt == 0:
                part = [
                    {k: row[k] for k in ("name", "role", "key_skills")}
                    for row in part
                    if row.get("role") == cfg.get("role")
                    and title_matches(
                        row["name"],
                        cfg["title_regex"],
                        cfg.get("c_title_regex"),
                        cfg.get("c_exclude_regex"),
                    )
                ]

            fail = inject_failure and task_id == 0 and attempt == 0
            future = pool.submit(
                map_chunk,
                part,
                cfg["title_regex"],
                cap,
                mode == "combined",
                fail,
                cfg.get("role"),
                cfg.get("c_title_regex"),
                cfg.get("c_exclude_regex"),
            )
            pending[future] = (task_id, part, attempt)

        exhausted = False

        while pending or not exhausted:
            while len(pending) < workers * 2 and not exhausted:
                try:
                    task_id, part = next(batches)
                except StopIteration:
                    exhausted = True
                else:
                    source_rows_read += len(part)
                    submit(task_id, part)

            if not pending:
                break

            done, _ = wait(pending, return_when=FIRST_COMPLETED)

            for future in done:
                task_id, part, attempt = pending.pop(future)

                try:
                    local, local_stats = future.result()
                except Exception as exc:
                    events.append(
                        {
                            "task_id": task_id,
                            "attempt": attempt,
                            "error": str(exc),
                        }
                    )

                    if attempt >= cfg["mapper_retries"]:
                        raise

                    submit(task_id, part, attempt + 1)
                else:
                    counts.update(local)
                    stats.update(local_stats)
                    stats["chunks"] += 1

    stats["rows_read"] = source_rows_read
    result = {
        "engine": f"mapreduce_{mode}",
        "variant": cfg["variant"],
        "cap": cap,
        "object_key": cfg["object_key"],
        "stats": dict(stats),
        "workers": workers,
        "chunk_rows": chunk_size,
        "events": events,
        "compute_seconds": perf_counter() - start,
        "counts": [
            {"skills": list(key[1:]), "count": value}
            for key, value in sorted(counts.items())
        ],
    }
    path = output or ROOT / f"results/counts/mapreduce_{mode}_k{cap}.json"
    save_json(path, result)

    if publish:
        upload(
            path,
            f"results/{cfg['variant']}/counts/mapreduce_{mode}_k{cap}.json",
        )

    print(
        f"MapReduce {mode} K={cap} вакансий {stats['vacancies']}", flush=True
    )

    return result
