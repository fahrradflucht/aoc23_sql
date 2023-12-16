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
        (CASE WHEN position('red' in game_set) > 0 THEN 
            substring(game_set from '(\d+) red')::int 
            ELSE 0 END) as red,
        (CASE WHEN position('green' in game_set) > 0 THEN
            substring(game_set from '(\d+) green')::int 
            ELSE 0 END) as green,
        (CASE WHEN position('blue' in game_set) > 0 THEN
            substring(game_set from '(\d+) blue')::int 
            ELSE 0 END) as blue
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
)
SELECT sum(id) as solution
FROM possible_games;