DROP SCHEMA IF EXISTS day1 CASCADE;
CREATE SCHEMA day1;

CREATE TABLE day1.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day1.inputs (value) FROM 'input.txt';

WITH first_digits AS (
    SELECT 
        d.id, 
        CASE 
            WHEN window_value ~ 'one' THEN '1'
            WHEN window_value ~ 'two' THEN '2'
            WHEN window_value ~ 'three' THEN '3'
            WHEN window_value ~ 'four' THEN '4'
            WHEN window_value ~ 'five' THEN '5'
            WHEN window_value ~ 'six' THEN '6'
            WHEN window_value ~ 'seven' THEN '7'
            WHEN window_value ~ 'eight' THEN '8'
            WHEN window_value ~ 'nine' THEN '9'
            WHEN window_value ~ '[1-9]'
                THEN regexp_replace(window_value, '[^1-9]', '', 'g')
            ELSE NULL
        END AS first_digit
    FROM 
        day1.inputs d
    CROSS JOIN LATERAL (
        SELECT SUBSTRING(d.value FOR n) as window_value
        FROM generate_series(1, LENGTH(d.value)) AS n
        WHERE SUBSTRING(d.value FOR n) ~ '(one|two|three|four|five|six|seven|eight|nine|[1-9])'
        LIMIT 1
    ) s
), last_digits AS (
    SELECT 
        d.id, 
        CASE 
            WHEN window_value ~ 'one' THEN '1'
            WHEN window_value ~ 'two' THEN '2'
            WHEN window_value ~ 'three' THEN '3'
            WHEN window_value ~ 'four' THEN '4'
            WHEN window_value ~ 'five' THEN '5'
            WHEN window_value ~ 'six' THEN '6'
            WHEN window_value ~ 'seven' THEN '7'
            WHEN window_value ~ 'eight' THEN '8'
            WHEN window_value ~ 'nine' THEN '9'
            WHEN window_value ~ '[0-9]'
                THEN regexp_replace(window_value, '[^1-9]', '', 'g')
            ELSE NULL
        END AS last_digit
    FROM 
        day1.inputs d
    CROSS JOIN LATERAL (
        SELECT SUBSTRING(d.value FROM n) as window_value
        FROM generate_series(LENGTH(d.value), 1, -1) AS n
        WHERE SUBSTRING(d.value FROM n) ~ '(one|two|three|four|five|six|seven|eight|nine|[1-9])'
        LIMIT 1
    ) s
)
SELECT
    SUM(
        (
            (SELECT first_digit FROM first_digits WHERE id = inputs.id)
            ||
            (SELECT last_digit FROM last_digits WHERE id = inputs.id)
        )::INTEGER
    ) AS sum_of_calibration_values
FROM day1.inputs;
