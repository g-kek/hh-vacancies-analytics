import os
from time import perf_counter
from urllib.parse import urlparse

import duckdb

from hh_analytics.config import ROOT, settings
from hh_analytics.storage import bucket, upload
from hh_analytics.utils import save_json


def connect():
    con = duckdb.connect()
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '1GB'")
    con.execute("LOAD httpfs")
    endpoint = urlparse(os.environ["S3_ENDPOINT"])

    def literal(s):
        return "'" + s.replace("'", "''") + "'"

    use_ssl = "true" if endpoint.scheme == "https" else "false"

    con.execute(f"""CREATE SECRET local_s3 (
        TYPE S3, KEY_ID {literal(os.environ["S3_ACCESS_KEY"])},
        SECRET {literal(os.environ["S3_SECRET_KEY"])},
        ENDPOINT {literal(endpoint.netloc)}, URL_STYLE 'path',
        REGION 'us-east-1', USE_SSL {use_ssl}
    )""")
    return con


def prepare(con):
    cfg = settings()
    con.execute(
        (ROOT / "sql/prepare.sql").read_text(),
        [
            f"s3://{bucket()}/{cfg['object_key']}",
            cfg["title_regex"],
            cfg.get("c_title_regex"),
            cfg.get("c_title_regex"),
            cfg.get("c_exclude_regex", "$^"),
            cfg.get("role"),
            cfg.get("role"),
        ],
    )


def count(con, cap):
    rows = con.execute(
        (ROOT / "sql/counts.sql").read_text(), [cap if cap else 2147483647]
    ).fetchall()
    return [
        {"skills": s, "count": n}
        for s, n in sorted(rows, key=lambda r: (len(r[0]), *r[0]))
    ]


def run(cap, output=None, publish=True):
    start = perf_counter()
    cfg = settings()
    with connect() as con:
        prepare(con)
        counts = count(con, cap)
        n, with_skills, truncated = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE len(skills)>0), "
            "count(*) FILTER (WHERE ? > 0 AND len(skills)>?) FROM vacancies",
            [cap, cap],
        ).fetchone()
    result = {
        "engine": "duckdb",
        "variant": cfg["variant"],
        "cap": cap,
        "object_key": cfg["object_key"],
        "stats": {
            "vacancies": n,
            "vacancies_with_skills": with_skills,
            "truncated_vacancies": truncated,
            **{
                f"emitted_{k}": sum(
                    r["count"] for r in counts if len(r["skills"]) == k
                )
                for k in (1, 2, 3)
            },
        },
        "compute_seconds": perf_counter() - start,
        "counts": counts,
    }
    path = output or ROOT / f"results/counts/duckdb_k{cap}.json"
    save_json(path, result)
    if publish:
        upload(path, f"results/{cfg['variant']}/counts/duckdb_k{cap}.json")
    print(f"DuckDB K={cap} вакансий {n}", flush=True)
    return result
