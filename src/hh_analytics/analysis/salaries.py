import hashlib
import itertools
import json
import random
import statistics

from hh_analytics.config import ROOT, settings
from hh_analytics.processing.duckdb import connect, prepare
from hh_analytics.storage import upload
from hh_analytics.utils import save_json, write_csv


def records(con, query, parameters=None):
    result = con.execute(query, parameters or [])
    return [
        dict(zip([c[0] for c in result.description], row))
        for row in result.fetchall()
    ]


def bootstrap_difference(treated, control, seed, attempts=1000):
    rng = random.Random(seed)
    estimates = sorted(
        statistics.median(rng.choices(treated, k=len(treated)))
        - statistics.median(rng.choices(control, k=len(control)))
        for _ in range(attempts)
    )

    return estimates[int(0.025 * attempts)], estimates[int(0.975 * attempts)]


def robust_checks(con, row):
    data = records(
        con,
        """SELECT vacancy_id, area, experience, net_salary,
                          list_has_all(skills, ?) AS treated
                          FROM salary_vacancies
                          WHERE net_salary IS NOT NULL AND
                          list_has_all(skills, ?)
""",
        [row["combo"], row["base"]],
    )
    yes = [r["net_salary"] for r in data if r["treated"]]
    no = [r["net_salary"] for r in data if not r["treated"]]

    row["n_without_added"] = len(no)
    row["median_without_added"] = statistics.median(no) if no else None
    row["exclusive_delta_rub"] = (
        statistics.median(yes) - statistics.median(no) if yes and no else None
    )

    if len(no) >= 10 and len(yes) >= 10:
        seed = int(
            hashlib.sha256(json.dumps(row["combo"]).encode()).hexdigest()[:8],
            16,
        )
        row["bootstrap_low"], row["bootstrap_high"] = bootstrap_difference(
            yes, no, seed
        )
    else:
        row["bootstrap_low"] = row["bootstrap_high"] = None

    strata = {}
    for r in data:
        groups = strata.setdefault(
            (r["area"], r["experience"]), {True: [], False: []}
        )
        groups[r["treated"]].append(r["net_salary"])

    comparable = [g for g in strata.values() if min(map(len, g.values())) >= 5]
    weights = [min(len(g[True]), len(g[False])) for g in comparable]

    row["comparable_strata"] = len(comparable)
    row["stratified_delta_rub"] = (
        sum(
            w * (statistics.median(g[True]) - statistics.median(g[False]))
            for w, g in zip(weights, comparable)
        )
        / sum(weights)
        if weights
        else None
    )

    return row


def run():
    cfg = settings()
    output = ROOT / "results/analysis"
    output.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        prepare(con)
        con.execute((ROOT / "sql/salaries.sql").read_text())
        overview = records(
            con,
            """SELECT count(*) AS vacancies,
            count(*) FILTER (WHERE len(skills)>0) AS with_skills,
            count(net_salary) AS n_net, median(net_salary) AS median_net,
            count(gross_salary) AS n_gross, median(gross_salary) AS
            median_gross,
            count(*) FILTER (WHERE amount IS NULL) AS missing_salary,
            count(*) FILTER (WHERE amount IS NOT NULL AND currency NOT IN
            ('RUR','RUB')) AS foreign_salary,
            count(*) FILTER (WHERE amount IS NOT NULL AND NOT
            valid_bounds) AS invalid_bounds,
            count(*) FILTER (WHERE amount IS NOT NULL AND currency IN
            ('RUR','RUB')
                            AND coalesce(gross,'') NOT IN
                            ('true','false')) AS unknown_gross,
            min(salary_from) AS min_salary_from, max(salary_to) AS
            max_salary_to,
            avg(len(skills)) AS mean_stack, max(len(skills)) AS max_stack
            FROM salary_vacancies""",
        )[0]
        save_json(output / "overview.json", overview)

        con.execute(
            "COPY (SELECT * FROM additions ORDER BY len(combo), combo) TO ? "
            "(FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(output / "salary_additions_all.parquet")],
        )
        selected = []
        for order, direction in itertools.product(
            (2, 3), ("positive", "negative")
        ):
            sign, sort = (
                (">", "DESC") if direction == "positive" else ("<", "ASC")
            )
            candidates = records(
                con,
                f"""SELECT * FROM additions
                WHERE len(combo)=? AND n_net>=? AND base_n_net>=?
                      AND base_n_net-n_net>=? AND delta_rub {sign} 0
                ORDER BY delta_rub {sort}, n_net DESC, combo LIMIT 5""",
                [
                    order,
                    cfg["min_salary_support"],
                    cfg["min_salary_support"],
                    cfg["min_salary_support"],
                ],
            )
            for row in candidates:
                selected.append(
                    {"direction": direction, **robust_checks(con, row)}
                )
        save_json(output / "salary_effects.json", selected)
        write_csv(
            output / "salary_effects.csv",
            (
                {
                    **r,
                    "combo": " + ".join(r["combo"]),
                    "base": " + ".join(r["base"]),
                }
                for r in selected
            ),
        )

        regions = cfg["regions"]
        summary = records(
            con,
            """SELECT area, count(*) AS vacancies,
            count(*) FILTER (WHERE len(skills)>0) AS with_skills,
            count(net_salary) AS n_net, median(net_salary) AS median_net,
            avg(net_salary) AS mean_net, quantile_cont(net_salary,.25) AS
            q25_net,
            quantile_cont(net_salary,.75) AS q75_net,
            count(gross_salary) AS n_gross, median(gross_salary) AS
            median_gross,
            avg(len(skills)) AS mean_stack,
            sum(len(skills)) AS singles_occurrences,
            sum(len(skills)*(len(skills)-1)/2)::BIGINT AS pairs_occurrences,
            sum(len(skills)*(len(skills)-1)*(len(skills)-2)/6)::BIGINT AS
            triples_occurrences
            FROM salary_vacancies WHERE area IN (SELECT unnest(?))
            GROUP BY area ORDER BY vacancies DESC""",
            [regions],
        )
        for row in summary:
            row["salary_coverage_pct"] = 100 * row["n_net"] / row["vacancies"]
        if len(summary) != 3:
            raise AssertionError("Нужны три непустых региона")
        write_csv(output / "regions_summary.csv", summary)
        skills = records(
            con,
            """WITH ranked AS (
            SELECT area, combo, len(combo) AS skill_order, count(*) AS
            frequency,
                   count(net_salary) AS n_net, median(net_salary) AS
                   median_net,
                   row_number() OVER (PARTITION BY area, len(combo)
                                      ORDER BY count(*) DESC, combo) AS rank
            FROM memberships WHERE area IN (SELECT unnest(?)) GROUP BY
            area, combo
        ) SELECT * FROM ranked WHERE rank<=10
        ORDER BY area, skill_order, rank""",
            [regions],
        )
        denominators = {r["area"]: r["vacancies"] for r in summary}
        for row in skills:
            row["vacancies_in_region"] = denominators[row["area"]]
            row["share_pct"] = (
                100 * row["frequency"] / denominators[row["area"]]
            )
        save_json(output / "regions_skills.json", skills)
        write_csv(
            output / "regions_skills.csv",
            ({**r, "combo": " + ".join(r["combo"])} for r in skills),
        )
        save_json(output / "regions_summary.json", summary)
    for path in sorted(output.iterdir()):
        upload(path, f"results/{cfg['variant']}/analysis/{path.name}")
    print(f"Анализ готов вакансий {overview['vacancies']}")
