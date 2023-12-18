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
), solution_part_1 AS (
    SELECT
        SUM(number::int) AS solution_part_1
    FROM extracted_numbers
    WHERE 
        SUBSTRING(previous_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]' OR
        SUBSTRING(current_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]' OR
        SUBSTRING(next_value FROM position - 1 FOR LENGTH(number) + 2) ~ '[^0-9.]'
), gear_parts AS (
    SELECT
        ec.*,
        array_agg(en.number) AS adjacent_numbers
    FROM 
        extracted_characters ec
    LEFT JOIN 
        extracted_numbers en
    ON 
        (ec.row_id BETWEEN en.row_id - 1 AND en.row_id + 1)
        AND 
        (ec.col_num BETWEEN en.position - 1 AND en.position + LENGTH(en.number))
    WHERE 
        ec.char = '*'
    GROUP BY 
        ec.row_id, ec.col_num, ec.char
    HAVING 
        COUNT(en.number) = 2
), solution_part_2 AS (
    SELECT
        SUM(adjacent_numbers[1]::int * adjacent_numbers[2]::int) AS solution_part_2
    FROM
        gear_parts
)
SELECT
    solution_part_1,
    solution_part_2
FROM
    solution_part_1,
    solution_part_2;
