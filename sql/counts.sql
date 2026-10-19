WITH limited AS (
    SELECT vacancy_id, list_slice(skills, 1, ?) AS skills FROM vacancies
), singles AS (
    SELECT vacancy_id, skills[i] AS a, i
    FROM limited, LATERAL range(1, len(skills) + 1) AS r(i)
), pairs AS (
    SELECT l.vacancy_id, l.skills[i] AS a, l.skills[j] AS b, i, j
    FROM limited l,
         LATERAL range(1, len(l.skills) + 1) AS r(i),
         LATERAL range(i + 1, len(l.skills) + 1) AS t(j)
), triples AS (
    SELECT l.vacancy_id, l.skills[i] AS a, l.skills[j] AS b, l.skills[k] AS c
    FROM limited l,
         LATERAL range(1, len(l.skills) + 1) AS r(i),
         LATERAL range(i + 1, len(l.skills) + 1) AS t(j),
         LATERAL range(j + 1, len(l.skills) + 1) AS u(k)
)
SELECT [a] AS skills, count(*)::BIGINT AS frequency FROM singles GROUP BY a
UNION ALL
SELECT [a, b], count(*)::BIGINT FROM pairs GROUP BY a, b
UNION ALL
SELECT [a, b, c], count(*)::BIGINT FROM triples GROUP BY a, b, c;
