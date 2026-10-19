import hashlib
import json

from hh_analytics.config import ROOT, settings
from hh_analytics.processing.mapreduce import run as count
from hh_analytics.storage import upload
from hh_analytics.utils import save_json, write_csv


def counts_dict(result):
    pairs = [(tuple(r["skills"]), r["count"]) for r in result["counts"]]
    if len({k for k, _ in pairs}) != len(pairs):
        raise AssertionError("Повторные ключи в результате")
    return dict(pairs)


def compare(a, b):
    left, right = counts_dict(a), counts_dict(b)
    errors = [
        (k, left.get(k), right.get(k))
        for k in left.keys() | right.keys()
        if left.get(k) != right.get(k)
    ]
    if errors:
        raise AssertionError(
            f"Расхождение частот ({len(errors)}): {errors[:10]}"
        )
    for field in (
        "vacancies",
        "vacancies_with_skills",
        "truncated_vacancies",
        "emitted_1",
        "emitted_2",
        "emitted_3",
    ):
        if a["stats"][field] != b["stats"][field]:
            raise AssertionError(f"Расхождение контрольного числа {field}")
    return hashlib.sha256(
        json.dumps(sorted(left.items()), ensure_ascii=False).encode()
    ).hexdigest()


def run():
    cfg = settings()
    checks, growth = [], []
    for cap in cfg["stack_limits"]:
        directory = ROOT / "results/counts"
        a = json.loads(
            (directory / f"mapreduce_baseline_k{cap}.json").read_text()
        )
        b = json.loads(
            (directory / f"mapreduce_combined_k{cap}.json").read_text()
        )
        c = json.loads((directory / f"duckdb_k{cap}.json").read_text())
        digest = compare(a, b)
        if digest != compare(b, c):
            raise AssertionError("Контрольные суммы не совпали")
        checks.append(
            {
                "cap": cap,
                "all_keys_equal": True,
                "sha256": digest,
                "unique_keys": len(b["counts"]),
                "stats": b["stats"],
            }
        )
        for order in (1, 2, 3):
            growth.append(
                {
                    "cap": cap,
                    "order": order,
                    "unique_combinations": sum(
                        len(r["skills"]) == order for r in b["counts"]
                    ),
                    "total_occurrences": b["stats"][f"emitted_{order}"],
                }
            )
    save_json(ROOT / "results/verification.json", checks)
    write_csv(ROOT / "results/combination_growth.csv", growth)
    upload(
        ROOT / "results/verification.json",
        f"results/{cfg['variant']}/verification.json",
    )
    for result in checks:
        print(f"K={result['cap']} совпало")
    return checks


def check_failure():
    cfg = settings()
    cap = max(cfg["stack_limits"])
    temporary = ROOT / ".runtime/failure_recovered.json"
    recovered = count(
        cap,
        inject_failure=True,
        output=temporary,
        publish=False,
    )
    temporary.unlink()
    original = json.loads(
        (ROOT / f"results/counts/mapreduce_combined_k{cap}.json").read_text()
    )
    digest = compare(original, recovered)
    if len(recovered["events"]) != 1:
        raise AssertionError("Ожидался один отказ и повтор задачи")
    result = {
        "injected_failures": 1,
        "retried_task": 0,
        "all_counts_equal": True,
        "sha256": digest,
        "events": recovered["events"],
        "scope": (
            "Исключение задачи маппера; не аварийное завершение ОС-процесса"
        ),
        "commit_policy": (
            "Редьюсер принимает результат только завершённой задачи целиком"
        ),
    }
    save_json(ROOT / "results/failure_test.json", result)
    upload(
        ROOT / "results/failure_test.json",
        f"results/{cfg['variant']}/failure_test.json",
    )
    print("Повтор маппера проверен")
