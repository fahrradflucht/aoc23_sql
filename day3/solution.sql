DROP SCHEMA IF EXISTS day3 CASCADE;
CREATE SCHEMA day3;

CREATE TABLE day3.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day3.inputs (value) FROM 'input.txt';

WITH extracted_characters AS (
    SELECT
        id AS row_id,
        col_num,
        char
    FROM day3.inputs
    CROSS JOIN LATERAL (
        SELECT
            generate_series(1, LENGTH(value)) AS col_num,
            SUBSTRING(value, generate_series(1, LENGTH(value)), 1) AS char
    )
), digit_group_start_markers AS (
    SELECT 
        row_id,
        col_num,
        char,
        char ~ '^[0-9]$' AND LAG(char) OVER (PARTITION BY row_id ORDER BY col_num) !~ '^[0-9]$' AS is_new_group
    FROM extracted_characters
), grouped_numbers AS (
    SELECT 
        row_id,
        col_num,
        char,
        COUNT(CASE WHEN is_new_group THEN 1 END) OVER (PARTITION BY row_id ORDER BY col_num) AS group_id
    FROM digit_group_start_markers
    WHERE char ~ '^[0-9]$'
), extracted_numbers AS (
    SELECT 
        gn.row_id,
        i.previous_value,
        i.current_value,
        i.next_value,
        MIN(gn.col_num) AS position,
        STRING_AGG(gn.char, '') AS number
    FROM grouped_numbers gn
    JOIN (
        SELECT 
            id,
            LAG(value) OVER (ORDER BY id) AS previous_value,
            value AS current_value,
            LEAD(value) OVER (ORDER BY id) AS next_value
        FROM day3.inputs
    ) i ON gn.row_id = i.id
    GROUP BY gn.row_id, gn.group_id, i.previous_value, i.current_value, i.next_value
)
SELECT
    SUM(number::int) as solution_part_1
FROM extracted_numbers
WHERE 
    SUBSTRING(previous_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]' OR
    SUBSTRING(current_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]' OR
    SUBSTRING(next_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]';