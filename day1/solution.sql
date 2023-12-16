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
            WHEN SUBSTRING(d.value FOR s.n) ~ 'one' THEN '1'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'two' THEN '2'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'three' THEN '3'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'four' THEN '4'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'five' THEN '5'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'six' THEN '6'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'seven' THEN '7'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'eight' THEN '8'
            WHEN SUBSTRING(d.value FOR s.n) ~ 'nine' THEN '9'
            WHEN SUBSTRING(d.value FOR s.n) ~ '[1-9]'
                THEN regexp_replace(SUBSTRING(d.value FOR s.n), '[^1-9]', '', 'g')
            ELSE NULL
        END AS first_digit
    FROM 
        day1.inputs d
    CROSS JOIN LATERAL (
        SELECT n
        FROM generate_series(1, LENGTH(d.value)) AS n
        WHERE SUBSTRING(d.value FOR n) ~ '(one|two|three|four|five|six|seven|eight|nine|[1-9])'
        LIMIT 1
    ) s
), last_digits AS (
    SELECT 
        d.id, 
        CASE 
            WHEN SUBSTRING(d.value FROM n) ~ 'one' THEN '1'
            WHEN SUBSTRING(d.value FROM n) ~ 'two' THEN '2'
            WHEN SUBSTRING(d.value FROM n) ~ 'three' THEN '3'
            WHEN SUBSTRING(d.value FROM n) ~ 'four' THEN '4'
            WHEN SUBSTRING(d.value FROM n) ~ 'five' THEN '5'
            WHEN SUBSTRING(d.value FROM n) ~ 'six' THEN '6'
            WHEN SUBSTRING(d.value FROM n) ~ 'seven' THEN '7'
            WHEN SUBSTRING(d.value FROM n) ~ 'eight' THEN '8'
            WHEN SUBSTRING(d.value FROM n) ~ 'nine' THEN '9'
            WHEN SUBSTRING(d.value FROM n) ~ '[0-9]'
                THEN regexp_replace(SUBSTRING(d.value FROM n), '[^1-9]', '', 'g')
            ELSE NULL
        END AS last_digit
    FROM 
        day1.inputs d
    CROSS JOIN LATERAL (
        SELECT n
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
