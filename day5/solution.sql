DROP SCHEMA IF EXISTS day5 CASCADE;
CREATE SCHEMA day5;

CREATE TABLE day5.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day5.inputs (value) FROM 'input.txt';

WITH text_groups AS (
    SELECT
        id,
        value,
        regexp_match(value, '^(\d+) (\d+) (\d+)$') AS map_data,
        COUNT(value = '' OR NULL) OVER (ORDER BY id) as group_id
    FROM
        day5.inputs
), seed_to_soil AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'seed-to-soil map:'
    ) AND array_length(map_data, 1) = 3
), soil_to_fertilizer AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'soil-to-fertilizer map:'
    ) AND array_length(map_data, 1) = 3
), fertilizer_to_water AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'fertilizer-to-water map:'
    ) AND array_length(map_data, 1) = 3
), water_to_light AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'water-to-light map:'
    ) AND array_length(map_data, 1) = 3
), light_to_temperature AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'light-to-temperature map:'
    ) AND array_length(map_data, 1) = 3
), temperature_to_humidity AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'temperature-to-humidity map:'
    ) AND array_length(map_data, 1) = 3
), humidity_to_location AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'humidity-to-location map:'
    ) AND array_length(map_data, 1) = 3
), seeds AS (
    SELECT
        string_to_table(
            regexp_replace(value, '^seeds: ', ''),
            ' '
        )::bigint AS seed
    FROM text_groups
    WHERE value ~ '^seeds:'
), soil AS (
    SELECT
        seed,
        CASE 
            WHEN sts.source_range_start IS NOT NULL THEN
                seeds.seed - sts.source_range_start + sts.dest_range_start
            ELSE
                seed
        END AS soil
    FROM seeds
    LEFT JOIN seed_to_soil sts ON
        seed BETWEEN sts.source_range_start AND sts.source_range_start + sts.range_length
), fertilizer AS (
    SELECT
        soil,
        CASE 
            WHEN stf.source_range_start IS NOT NULL THEN
                soil.soil - stf.source_range_start + stf.dest_range_start
            ELSE
                soil
        END AS fertilizer
    FROM soil
    LEFT JOIN soil_to_fertilizer stf ON
        soil BETWEEN stf.source_range_start AND stf.source_range_start + stf.range_length
), water AS (
    SELECT
        fertilizer,
        CASE 
            WHEN ftw.source_range_start IS NOT NULL THEN
                fertilizer.fertilizer - ftw.source_range_start + ftw.dest_range_start
            ELSE
                fertilizer
        END AS water
    FROM fertilizer
    LEFT JOIN fertilizer_to_water ftw ON
        fertilizer BETWEEN ftw.source_range_start AND ftw.source_range_start + ftw.range_length
), light AS (
    SELECT
        water,
        CASE 
            WHEN wtl.source_range_start IS NOT NULL THEN
                water.water - wtl.source_range_start + wtl.dest_range_start
            ELSE
                water
        END AS light
    FROM water
    LEFT JOIN water_to_light wtl ON
        water BETWEEN wtl.source_range_start AND wtl.source_range_start + wtl.range_length
), temperature AS (
    SELECT
        light,
        CASE 
            WHEN ltt.source_range_start IS NOT NULL THEN
                light.light - ltt.source_range_start + ltt.dest_range_start
            ELSE
                light
        END AS temperature
    FROM light
    LEFT JOIN light_to_temperature ltt ON
        light BETWEEN ltt.source_range_start AND ltt.source_range_start + ltt.range_length
), humidity AS (
    SELECT
        temperature,
        CASE 
            WHEN tth.source_range_start IS NOT NULL THEN
                temperature.temperature - tth.source_range_start + tth.dest_range_start
            ELSE
                temperature
        END AS humidity
    FROM temperature
    LEFT JOIN temperature_to_humidity tth ON
        temperature BETWEEN tth.source_range_start AND tth.source_range_start + tth.range_length
), location AS (
    SELECT
        humidity,
        CASE 
            WHEN htl.source_range_start IS NOT NULL THEN
                humidity.humidity - htl.source_range_start + htl.dest_range_start
            ELSE
                humidity
        END AS location
    FROM humidity
    LEFT JOIN humidity_to_location htl ON
        humidity BETWEEN htl.source_range_start AND htl.source_range_start + htl.range_length
)
SELECT
    MIN(location) AS solution_part_1
FROM location;
