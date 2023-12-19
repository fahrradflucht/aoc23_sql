DROP SCHEMA IF EXISTS day4 CASCADE;
CREATE SCHEMA day4;

CREATE TABLE day4.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day4.inputs (value) FROM 'input.txt';

-- While this would also work with a CTE, adding a temporary table to be able to
-- index the PK makes the run-time for part 2 more bareable.
CREATE TEMPORARY TABLE cards AS
SELECT
    matches[1]::int AS card_id,
    array_length(
        ARRAY (
            SELECT string_to_table(matches[2], ' ')
            INTERSECT
            SELECT string_to_table(matches[3], ' ')
        ),
        1
    ) AS matches_count
FROM (
    SELECT
        regexp_match(
            regexp_replace(value, ' +', ' ', 'g'),
            'Card (\d+): ([0-9 ]+) \| ([0-9 ]+)'
        ) AS matches
    FROM day4.inputs
);

ALTER TABLE cards ADD PRIMARY KEY (card_id);

WITH RECURSIVE cards_with_copies AS (
    SELECT
        card_id,
        matches_count
    FROM cards
    UNION ALL
    SELECT
        cards.card_id,
        cards.matches_count
    FROM cards
    JOIN cards_with_copies ON cards.card_id
        BETWEEN
            cards_with_copies.card_id + 1
        AND
            cards_with_copies.card_id + cards_with_copies.matches_count
)
SELECT
    (SELECT SUM(2^(matches_count - 1)) FROM cards) as solution_part_1,
    (SELECT COUNT(*) FROM cards_with_copies) as solution_part_2
;
