DROP SCHEMA IF EXISTS day5 CASCADE;
CREATE SCHEMA day5;

CREATE TABLE day5.inputs (
    id      SERIAL,
    value   TEXT NOT NULL
);

\COPY day5.inputs (value) FROM 'input.txt';

CREATE TEMPORARY TABLE text_groups AS (
    SELECT
        id,
        value,
        regexp_match(value, '^(\d+) (\d+) (\d+)$') AS map_data,
        COUNT(value = '' OR NULL) OVER (ORDER BY id) as group_id
    FROM
        day5.inputs
);

CREATE TEMPORARY TABLE seed_to_soil AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'seed-to-soil map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX seed_to_soil_source_range_idx ON seed_to_soil USING GIST (source_range);

CREATE TEMPORARY TABLE soil_to_fertilizer AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'soil-to-fertilizer map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX soil_to_fertilizer_source_range_idx ON soil_to_fertilizer USING GIST (source_range);

CREATE TEMPORARY TABLE fertilizer_to_water AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'fertilizer-to-water map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX fertilizer_to_water_source_range_idx ON fertilizer_to_water USING GIST (source_range);

CREATE TEMPORARY TABLE water_to_light AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'water-to-light map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX water_to_light_source_range_idx ON water_to_light USING GIST (source_range);

CREATE TEMPORARY TABLE light_to_temperature AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'light-to-temperature map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX light_to_temperature_source_range_idx ON light_to_temperature USING GIST (source_range);

CREATE TEMPORARY TABLE temperature_to_humidity AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'temperature-to-humidity map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX temperature_to_humidity_source_range_idx ON temperature_to_humidity USING GIST (source_range);

CREATE TEMPORARY TABLE humidity_to_location AS (
    SELECT
        map_data[1]::bigint AS dest_range_start,
        map_data[2]::bigint AS source_range_start,
        map_data[3]::bigint AS range_length,
        int8range(
            map_data[2]::bigint,
            map_data[2]::bigint + map_data[3]::bigint
        ) AS source_range
    FROM text_groups
    WHERE group_id IN (
        SELECT group_id
        FROM text_groups
        WHERE value = 'humidity-to-location map:'
    ) AND array_length(map_data, 1) = 3
);
CREATE INDEX humidity_to_location_source_range_idx ON humidity_to_location USING GIST (source_range);

WITH seed_data AS (
    SELECT 
        seed::bigint,
        row_number() OVER (ORDER BY ordinality) AS row_number,
        (lead(seed) OVER (ORDER BY ordinality))::bigint AS range
    FROM 
        text_groups,
        regexp_split_to_table(
            regexp_replace(value, '^seeds: ', ''),
            ' '
        ) WITH ordinality AS seed
    WHERE value ~ '^seeds:'
), seeds_from_ranges AS (
    SELECT 
        generate_series(seed, seed + range - 1) as seed
    FROM seed_data
    WHERE row_number % 2 = 1
), seeds AS (
    SELECT
        seed,
        true AS range_based
    FROM seeds_from_ranges
    UNION ALL
    SELECT
        seed,
        false AS range_based
    FROM seed_data
), soil AS (
    SELECT
        seed,
        CASE 
            WHEN sts.source_range_start IS NOT NULL THEN
                seeds.seed - sts.source_range_start + sts.dest_range_start
            ELSE
                seed
        END AS soil,
        range_based
    FROM seeds
    LEFT JOIN seed_to_soil sts ON
        seed <@ sts.source_range
), fertilizer AS (
    SELECT
        soil,
        CASE 
            WHEN stf.source_range_start IS NOT NULL THEN
                soil.soil - stf.source_range_start + stf.dest_range_start
            ELSE
                soil
        END AS fertilizer,
        range_based
    FROM soil
    LEFT JOIN soil_to_fertilizer stf ON
        soil <@ stf.source_range
), water AS (
    SELECT
        fertilizer,
        CASE 
            WHEN ftw.source_range_start IS NOT NULL THEN
                fertilizer.fertilizer - ftw.source_range_start + ftw.dest_range_start
            ELSE
                fertilizer
        END AS water,
        range_based
    FROM fertilizer
    LEFT JOIN fertilizer_to_water ftw ON
        fertilizer <@ ftw.source_range
), light AS (
    SELECT
        water,
        CASE 
            WHEN wtl.source_range_start IS NOT NULL THEN
                water.water - wtl.source_range_start + wtl.dest_range_start
            ELSE
                water
        END AS light,
        range_based
    FROM water
    LEFT JOIN water_to_light wtl ON
        water <@ wtl.source_range
), temperature AS (
    SELECT
        light,
        CASE 
            WHEN ltt.source_range_start IS NOT NULL THEN
                light.light - ltt.source_range_start + ltt.dest_range_start
            ELSE
                light
        END AS temperature,
        range_based
    FROM light
    LEFT JOIN light_to_temperature ltt ON
        light <@ ltt.source_range
), humidity AS (
    SELECT
        temperature,
        CASE 
            WHEN tth.source_range_start IS NOT NULL THEN
                temperature.temperature - tth.source_range_start + tth.dest_range_start
            ELSE
                temperature
        END AS humidity,
        range_based
    FROM temperature
    LEFT JOIN temperature_to_humidity tth ON
        temperature <@ tth.source_range
), location AS (
    SELECT
        humidity,
        CASE 
            WHEN htl.source_range_start IS NOT NULL THEN
                humidity.humidity - htl.source_range_start + htl.dest_range_start
            ELSE
                humidity
        END AS location,
        range_based
    FROM humidity
    LEFT JOIN humidity_to_location htl ON
        humidity <@ htl.source_range
)
SELECT
    (SELECT MIN(location) AS solution_part_1 FROM location WHERE range_based = false) AS solution_part_1,
    (SELECT MIN(location) AS solution_part_2 FROM location WHERE range_based = true) AS solution_part_2
;
