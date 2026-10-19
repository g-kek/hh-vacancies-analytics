import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def settings():
    with (ROOT / "config/analysis.json").open() as f:
        cfg = json.load(f)
    if not cfg.get("variant") or not cfg.get("title_regex"):
        raise ValueError(
            (
                "Укажите индивидуальный вариант и title_regex в "
                "config/analysis.json"
            )
        )
    return cfg
