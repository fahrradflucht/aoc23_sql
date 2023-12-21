DROP SCHEMA IF EXISTS day6 CASCADE;
CREATE SCHEMA day6;

CREATE TABLE day6.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day6.inputs (value) FROM 'input.txt';

CREATE AGGREGATE day6.mul(bigint) ( SFUNC = int8mul, STYPE=bigint );

WITH times AS (
    SELECT
        ordinality as id,
        time::int AS time
    FROM
        day6.inputs,
        regexp_split_to_table(
            regexp_replace(value, '^Time: +', ''),
            ' +'
        ) WITH ordinality AS time
    WHERE value ~ '^Time: '
), distances AS (
    SELECT
        ordinality as id,
        distance::int AS distance
    FROM
        day6.inputs,
        regexp_split_to_table(
            regexp_replace(value, '^Distance: +', ''),
            ' +'
        ) WITH ordinality AS distance
    WHERE value ~ '^Distance: '
), races AS (
    SELECT
        times.id,
        times.time,
        distances.distance
    FROM
        times
    JOIN distances ON times.id = distances.id
), strategies AS (
    SELECT
        races.id as race_id,
        button_hold_time,
        button_hold_time * (races.time - button_hold_time) AS distance,
        (button_hold_time * (races.time - button_hold_time)) > races.distance AS winning_strategy
    FROM
        races,
        generate_series(0, races.time) AS button_hold_time
), race_margins AS (
    SELECT
        race_id,
        COUNT(*) FILTER (WHERE winning_strategy) AS margin
    FROM
        strategies
    GROUP BY
        race_id
)
SELECT
    day6.mul(margin) AS margin_product
FROM
    race_margins
;
