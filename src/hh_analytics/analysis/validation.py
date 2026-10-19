import itertools
import json
import math
import statistics
from collections import Counter

from hh_analytics.config import ROOT, settings
from hh_analytics.processing.duckdb import connect, prepare
from hh_analytics.processing.mapreduce import normalize_skills, title_matches
from hh_analytics.storage import stream_csv, upload
from hh_analytics.utils import save_json


def salary(row):
    def number(s):
        return float(s) if s else None

    low, high = number(row["salary_from"]), number(row["salary_to"])
    if row["currency"] not in ("RUR", "RUB") or row["gross"] != "false":
        return None
    if (low is not None and low <= 0) or (high is not None and high <= 0):
        return None
    if low is not None and high is not None and low > high:
        return None
    value = (
        (low + high) / 2
        if low is not None and high is not None
        else (low if low is not None else high)
    )
    return (
        value
        if value is not None and math.isfinite(value) and value > 0
        else None
    )


def median(values):
    values = [x for x in values if x is not None]
    return statistics.median(values) if values else None


def run():
    cfg = settings()
    rows = []
    for row in stream_csv(cfg["object_key"]):
        if row["role"] == cfg["role"] and title_matches(
            row["name"],
            cfg["title_regex"],
            cfg["c_title_regex"],
            cfg["c_exclude_regex"],
        ):
            rows.append(
                {
                    **row,
                    "skills": normalize_skills(row["key_skills"]),
                    "net": salary(row),
                }
            )
    with connect() as con:
        prepare(con)
        sql_ids = {
            r[0]
            for r in con.execute("SELECT vacancy_id FROM vacancies").fetchall()
        }
    if sql_ids != {r["vacancy_id"] for r in rows}:
        raise AssertionError("SQL и Python отобрали разные ID")
    output = ROOT / "results/analysis"
    summary = json.loads((output / "regions_summary.json").read_text())
    region_skills = json.loads((output / "regions_skills.json").read_text())
    effects = json.loads((output / "salary_effects.json").read_text())
    for region in summary:
        group = [r for r in rows if r["area"] == region["area"]]
        assert len(group) == region["vacancies"]
        assert sum(r["net"] is not None for r in group) == region["n_net"]
        assert median([r["net"] for r in group]) == region["median_net"]
        for k, field in (
            (1, "singles_occurrences"),
            (2, "pairs_occurrences"),
            (3, "triples_occurrences"),
        ):
            assert (
                sum(math.comb(len(r["skills"]), k) for r in group)
                == region[field]
            )
    for entry in region_skills:
        group = [
            r
            for r in rows
            if r["area"] == entry["area"]
            and set(entry["combo"]) <= set(r["skills"])
        ]
        assert len(group) == entry["frequency"]
        assert median([r["net"] for r in group]) == entry["median_net"]
    frequencies = Counter(
        c
        for r in rows
        for k in (1, 2)
        for c in itertools.combinations(r["skills"], k)
    )
    for entry in effects:
        base = tuple(entry["base"])
        possibilities = list(itertools.combinations(entry["combo"], len(base)))
        expected_base = min(possibilities, key=lambda c: (-frequencies[c], c))
        assert base == expected_base
        for combo, n, med in (
            (entry["combo"], entry["n_net"], entry["median_net"]),
            (base, entry["base_n_net"], entry["base_median_net"]),
        ):
            values = [
                r["net"]
                for r in rows
                if set(combo) <= set(r["skills"]) and r["net"] is not None
            ]
            assert len(values) == n and median(values) == med
    net = [r["net"] for r in rows if r["net"] is not None]
    outliers = [
        {
            "vacancy_id": r["vacancy_id"],
            "name": r["name"],
            "area": r["area"],
            "net_salary": r["net"],
        }
        for r in rows
        if r["net"] is not None and not 10000 <= r["net"] <= 2000000
    ]
    evidence = {
        "filter_ids_equal": True,
        "vacancies": len(rows),
        "independently_checked_regional_rows": len(summary),
        "independently_checked_regional_skill_rows": len(region_skills),
        "independently_checked_salary_effect_rows": len(effects),
        "outlier_policy": "Первичный расчёт сохраняет выбросы; "
        "чувствительность: 10000..2000000 руб.",
        "outliers": outliers,
        "median_net_original": median(net),
        "median_net_sensitivity": median(
            [x for x in net if 10000 <= x <= 2000000]
        ),
        "sample_size_net_original": len(net),
        "sample_size_net_sensitivity": sum(10000 <= x <= 2000000 for x in net),
        "full_stack_occurrences": {
            k: sum(math.comb(len(r["skills"]), k) for r in rows)
            for k in (1, 2, 3)
        },
    }
    save_json(ROOT / "results/analysis_validation.json", evidence)
    upload(
        ROOT / "results/analysis_validation.json",
        f"results/{cfg['variant']}/analysis_validation.json",
    )
    print("Анализ проверен")
