CREATE TEMP TABLE vacancies AS
SELECT source, vacancy_id, name, area, experience, currency, gross,
       TRY_CAST(salary_from AS DOUBLE) AS salary_from,
       TRY_CAST(salary_to AS DOUBLE) AS salary_to,
       list_sort(list_distinct(list_filter(
           list_transform(string_split(coalesce(key_skills, ''), ';'),
                          s -> lower(trim(s, chr(32) || chr(9) || chr(13) || chr(10)))),
           s -> s <> ''))) AS skills
FROM read_csv(?, header = true, all_varchar = true)
WHERE (regexp_matches(lower(name), ?)
       OR (? IS NOT NULL AND regexp_matches(name, ?)
           AND NOT regexp_matches(lower(name), coalesce(?, '$^'))))
  AND (? IS NULL OR role = ?);
