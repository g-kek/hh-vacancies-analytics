import csv
import json
import zipfile
from collections import Counter
from pathlib import Path

from hh_analytics.config import ROOT, settings
from hh_analytics.storage import create_bucket, upload
from hh_analytics.utils import save_json

FIELDS = [
    "source",
    "vacancy_id",
    "snapshot_date",
    "published_at",
    "name",
    "role",
    "area",
    "key_skills",
    "salary_from",
    "salary_to",
    "currency",
    "gross",
    "experience",
]


def prepare(csv_path, zip_path, extra_path):
    paths = [Path(csv_path), Path(zip_path), Path(extra_path)]
    create_bucket()

    output = ROOT / "data/vacancies.csv"
    output.parent.mkdir(exist_ok=True)
    ids = {}
    audit = Counter()
    dates = Counter()
    currencies = Counter()
    with paths[0].open(newline="") as src, output.open("w", newline="") as dst:
        reader = csv.DictReader(src)
        if not set(FIELDS).issubset(reader.fieldnames):
            raise ValueError("В основном CSV нет обязательных столбцов")
        writer = csv.DictWriter(dst, FIELDS)
        writer.writeheader()
        for row in reader:
            ident = (row["source"], row["vacancy_id"])
            if ident in ids:
                raise ValueError(f"Повторный идентификатор: {ident}")
            ids[ident] = (row["name"], row["key_skills"])
            writer.writerow({k: row[k] for k in FIELDS})
            audit["csv_rows"] += 1
            dates[row["snapshot_date"]] += 1
            currencies[row["currency"] or "missing"] += 1

        with zipfile.ZipFile(paths[1]) as archive:
            for name in sorted(archive.namelist()):
                if not name.endswith(".json"):
                    continue
                row = json.loads(archive.read(name))
                audit["archive_rows"] += 1
                ident = ("hh", str(row["vacancy_id"]))
                if ident in ids:
                    audit["archive_overlaps"] += 1
                    original_title, original_skills = ids[ident]
                    audit["archive_title_matches"] += (
                        original_title == row["name"]
                    )
                    a = {
                        s.strip().lower()
                        for s in original_skills.split(";")
                        if s.strip()
                    }
                    b = {
                        s.strip().lower()
                        for s in row["key_skills"]
                        if s.strip()
                    }
                    audit["archive_skill_matches"] += a == b
                else:
                    raise ValueError(
                        f"Архив содержит ID вне основного CSV: {ident}"
                    )

    with paths[2].open(newline="") as f:
        reader = csv.DictReader(f)
        extra_columns = reader.fieldnames
        extra_rows = sum(1 for _ in reader)
    if set(FIELDS).issubset(extra_columns):
        raise ValueError(
            "Дополнительный CSV изменился: пересмотрите правила объединения"
        )
    manifest = {
        "audit": dict(audit),
        "snapshot_dates": dict(sorted(dates.items())),
        "currencies": dict(currencies),
        "bronze_columns": FIELDS,
        "extra_file": {
            "name": paths[2].name,
            "columns": extra_columns,
            "rows": extra_rows,
            "used_in_analysis": False,
            "reason": "Нет идентификаторов вакансий, навыков, регионов и "
            "зарплат",
        },
        "archive_policy": "Аудит 10000 JSON по ID, названию и навыкам; при"
        " пересечении берётся CSV",
        "inputs": [],
    }
    for path in paths + [output]:
        key = ("bronze/" if path == output else "raw/") + path.name
        info = upload(path, key)
        manifest["inputs"].append({"name": path.name, **info})
    save_json(ROOT / "results/data_manifest.json", manifest)
    upload(ROOT / "results/data_manifest.json", "metadata/data_manifest.json")
    print(f"Загружено {len(paths) + 1} файла", flush=True)
    return manifest


def load():
    create_bucket()
    upload(ROOT / "data/vacancies.csv", settings()["object_key"])
    print("Данные загружены")
