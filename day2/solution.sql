DROP SCHEMA IF EXISTS day2 CASCADE;
CREATE SCHEMA day2;

CREATE TABLE day2.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day2.inputs (value) FROM 'input.txt';

WITH game_sets AS (
    SELECT
        id,
        COALESCE((substring(game_set from '(\d+) red'))::int, 0) as red,
        COALESCE((substring(game_set from '(\d+) green'))::int, 0) as green,
        COALESCE((substring(game_set from '(\d+) blue'))::int, 0) as blue
    FROM (
        SELECT id, unnest(
            string_to_array(
                regexp_replace(value, '^Game [1-9]+: ', '', 'g'),
                '; '
            )
        ) as game_set
        FROM day2.inputs
    ) raw_game_sets
), possible_games AS (
    SELECT id
    FROM game_sets
    GROUP BY id
    HAVING bool_and(red <= 12 AND green <= 13 AND blue <= 14) = true
), minimum_sets AS (
    SELECT
        id,
        max(red) * max(green) * max(blue) as power
    FROM game_sets
    GROUP BY id
)
SELECT
    (SELECT SUM(id) FROM possible_games) as solution_part_1,
    (SELECT SUM(power) FROM minimum_sets) as solution_part_2;