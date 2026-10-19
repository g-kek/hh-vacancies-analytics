CREATE TEMP TABLE salary_vacancies AS
WITH bounds AS (
    SELECT *, CASE
        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
            THEN (salary_from + salary_to) / 2
        ELSE coalesce(salary_from, salary_to)
    END AS amount,
    (salary_from IS NULL OR salary_from > 0)
    AND (salary_to IS NULL OR salary_to > 0)
    AND (salary_from IS NULL OR salary_to IS NULL OR salary_from <= salary_to) AS valid_bounds
    FROM vacancies
)
SELECT *, CASE WHEN currency IN ('RUR', 'RUB') AND valid_bounds
                         AND isfinite(amount) AND amount > 0
                         AND lower(gross) = 'false' THEN amount END AS net_salary,
          CASE WHEN currency IN ('RUR', 'RUB') AND valid_bounds
                         AND isfinite(amount) AND amount > 0
                         AND lower(gross) = 'true' THEN amount END AS gross_salary
FROM bounds;

CREATE TEMP TABLE memberships AS
SELECT vacancy_id, area, experience, net_salary, gross_salary,
       [skills[i]] AS combo
FROM salary_vacancies, LATERAL range(1, len(skills) + 1) AS a(i)
UNION ALL
SELECT vacancy_id, area, experience, net_salary, gross_salary,
       [skills[i], skills[j]]
FROM salary_vacancies,
     LATERAL range(1, len(skills) + 1) AS a(i),
     LATERAL range(i + 1, len(skills) + 1) AS b(j)
UNION ALL
SELECT vacancy_id, area, experience, net_salary, gross_salary,
       [skills[i], skills[j], skills[k]]
FROM salary_vacancies,
     LATERAL range(1, len(skills) + 1) AS a(i),
     LATERAL range(i + 1, len(skills) + 1) AS b(j),
     LATERAL range(j + 1, len(skills) + 1) AS c(k);

CREATE TEMP TABLE combo_stats AS
SELECT combo, count(*) AS n_all, count(net_salary) AS n_net,
       median(net_salary) AS median_net, count(gross_salary) AS n_gross,
       median(gross_salary) AS median_gross
FROM memberships GROUP BY combo;

CREATE TEMP TABLE additions AS
WITH candidates AS (
    SELECT g.*, b.combo AS base, b.n_all AS base_n_all,
           b.n_net AS base_n_net, b.median_net AS base_median_net,
           b.n_gross AS base_n_gross, b.median_gross AS base_median_gross,
           row_number() OVER (PARTITION BY g.combo ORDER BY b.n_all DESC, b.combo) AS rank
    FROM combo_stats g,
         LATERAL range(1, len(g.combo) + 1) AS removed(i),
         LATERAL (SELECT * FROM combo_stats b
                  WHERE b.combo = list_concat(list_slice(g.combo, 1, i - 1),
                                             list_slice(g.combo, i + 1, len(g.combo)))) b
    WHERE len(g.combo) IN (2, 3)
)
SELECT * EXCLUDE (rank),
       list_filter(combo, s -> NOT list_contains(base, s))[1] AS added_skill,
       median_net - base_median_net AS delta_rub,
       100 * (median_net / nullif(base_median_net, 0) - 1) AS delta_pct,
       median_gross - base_median_gross AS gross_delta_rub
FROM candidates WHERE rank = 1;
