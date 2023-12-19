DROP SCHEMA IF EXISTS day4 CASCADE;
CREATE SCHEMA day4;

CREATE TABLE day4.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day4.inputs (value) FROM 'input.txt';

WITH games AS (
    SELECT
        array_length(
            ARRAY (
                SELECT string_to_table(matches[1], ' ')
                INTERSECT
                SELECT string_to_table(matches[2], ' ')
            ),
            1
        ) AS matches_count
    FROM (
        SELECT
            regexp_match(
                regexp_replace(value, ' +', ' ', 'g'),
                'Card \d+: ([0-9 ]+) \| ([0-9 ]+)'
            ) AS matches
        FROM day4.inputs
    )
)
SELECT
    SUM(2^(matches_count - 1)) as solution_part_1
FROM games;
