--SUMMARY STATISTICS (FIND OUT THE WHO)
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.composition_statistics_summary` AS
SELECT
    -- Grouping columns
    member_casual,
    rideable_type,

    -- 1. Standard Aggregations (Count, Min, Mean)
    COUNT(ride_id) AS total_rides_in_composition,
    
    -- Calculate percentage
    ROUND(
        COUNT(ride_id) * 100 / SUM(COUNT(ride_id)) OVER (), 2
    ) AS percentage_of_total_rides,
    
    -- Duration Metrics in Minutes
    ROUND(AVG(ride_length_minutes), 0) AS avg_ride_length_minutes,
    ROUND(MIN(ride_length_minutes), 0) AS min_ride_length_minutes,
    
    -- 2. Max Duration formatted as H:M (FIXED CALCULATION)
    FORMAT_TIME(
        "%H:%M",
        -- 1. Convert total minutes to seconds and get a TIMESTAMP
        TIME(TIMESTAMP_SECONDS(CAST(MAX(ride_length_minutes) * 60 AS INT64))) 
        -- 2. Use TIME() to extract the TIME object, to avoid type mismatch.
    ) AS max_ride_length_h_m,


    -- 3. Median and Quartiles (Using APPROX_QUANTILES)
    -- P25 (First Quartile)
    ROUND(
        APPROX_QUANTILES(ride_length_minutes, 4)[OFFSET(1)], 0
    ) AS p25_quartile_minutes,

    -- P50 (Median)
    ROUND(
        APPROX_QUANTILES(ride_length_minutes, 4)[OFFSET(2)], 0
    ) AS median_ride_length_minutes,

    -- P75 (Third Quartile)
    ROUND(
        APPROX_QUANTILES(ride_length_minutes, 4)[OFFSET(3)], 0
    ) AS p75_quartile_minutes
    
FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
GROUP BY
    1, 2
ORDER BY
    avg_ride_length_minutes DESC;


--FIND OUT THE WHEN 
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.peak_frequency` AS
-- Pivot Table 2: Frequency of High-Value Segment by Day and Hour
-- Target: Casual Classic (P75 Duration: 30.0 minutes)
SELECT
    -- Grouping fields for the matrix
    member_casual,
    rideable_type,
    day_of_week_name,
    trip_hour,

    -- Metric 1: Absolute Count (Frequency)
    COUNT(ride_id) AS Total_Rides_Count,

    -- Metric 2: Percentage of Total Casual Classic Rides (for easy comparison)
    -- This uses a window function to divide the hourly count by the total count for the segment
    COUNT(ride_id) * 100.0 / SUM(COUNT(ride_id)) OVER() AS Percentage_of_Casual_Classic

FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`

-- Filter down only to the target segment identified in summary table
WHERE
    member_casual = 'casual'
    AND rideable_type = 'classic_bike'

GROUP BY
    1, 2, 3, 4 -- Group by the filters, day_of_week, and trip_hour
ORDER BY
    Total_Rides_Count DESC,
    trip_hour ASC;


CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.proof_of_concept` AS
--FIND OUT WHY? PROVE THE HYPOTHESIS
-- Pivot Table 3: P75 Duration Split by Day Type (Weekday vs. Weekend)
SELECT
    member_casual,
    rideable_type,
    -- 1. Create the Day Type grouping field (Weekend = Sunday/Saturday)
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,

    -- 2. Metric 1: Count (to confirm sample size)
    COUNT(ride_id) AS Total_Rides_Count,

    -- 3. Metric 2: P50 Median (Reliable Benchmark)
    -- APPROX_QUANTILES(column, 4) returns an array [Min, P25, P50, P75, Max].
    -- P50 (Median) is at index 2 (OFFSET(2)).
    ROUND(
        APPROX_QUANTILES(ride_length_minutes, 4)[OFFSET(2)], 0
    ) AS P50_Median_Minutes,

    -- 4. Metric 3: P75 Quartile (The proof of concept)
    -- P75 is at index 3 (OFFSET(3)).
    ROUND(
        APPROX_QUANTILES(ride_length_minutes, 4)[OFFSET(3)], 0
    ) AS P75_Quartile_Minutes

FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`

-- Filter only to the high-value target segment
WHERE
    member_casual = 'casual'
    AND rideable_type = 'classic_bike'

-- Group by the calculated day_type to aggregate the metrics correctly
GROUP BY
    1, 2, 3
ORDER BY
    P75_Quartile_Minutes DESC;


-- TEMPORAL EXTREMES (TIME SEGMENT)
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.all_temporal_extremes` AS
SELECT
    -- Grouping Columns (Compositions)
    member_casual,
    rideable_type,
    
    -- Temporal Grouping Columns
    trip_month_year,        -- Added for Seasonal Extremes
    trip_hour,              -- 0-23
    day_of_week_name,       -- Monday, Tuesday, etc.
    time_of_day_category,   -- Morning, Afternoon, Evening, Night
    day_type,               -- Weekday vs. Weekend
    
    -- Metrics
    COUNT(ride_id) AS total_rides_in_composition,
    
FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 
    member_casual,
    rideable_type,
    trip_month_year,        -- Chronological sorting
    day_of_week_name,
    trip_hour,
    total_rides_in_composition DESC;



/**
    MISCELLANEOUS
    
    EXPLORING THE DATASET FOR OTHER INSIGHT BEYOND TIME SERIES ANALYSIS

    POTENTIAL EXPLORATION:
    1. TRIP DURATION BASED ON LOGICAL TYPE OF COMMUTE
    2. ONE WAY TRIP
    3. ROUND TRIP
    4. ROUND TRIP VS ONE WAY TRIP
    5. RIDE BY START STATION
    6. RIDE BY END STATION
**/


--CATEGORIZE TRIP DURATION INTO BUCKET
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.duration_bucket` AS
SELECT
    member_casual,
    rideable_type,
    
    -- 1. Short Commute/Errand (1-15 min)
    COUNT(CASE WHEN ride_length_minutes BETWEEN 1 AND 15 THEN ride_id END) AS short_commute_count,
    
    -- 2. Standard Leisure/Transit (16-60 min)
    COUNT(CASE WHEN ride_length_minutes BETWEEN 16 AND 60 THEN ride_id END) AS standard_leisure_count,
    
    -- 3. Long Leisure/Fitness (61-180 min)
    COUNT(CASE WHEN ride_length_minutes BETWEEN 61 AND 180 THEN ride_id END) AS long_leisure_count,
    
    -- 4. Ultra-Extended Use (181-1440 min)
    COUNT(CASE WHEN ride_length_minutes > 180 THEN ride_id END) AS ultra_extended_count,   
FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
GROUP BY 1, 2;


-- AGGREGATE ROUTES FOR ONE WAY TRIP
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.aggregated_one_way_trip` AS
WITH AggregatedRoutes AS (
    -- Step 1: Group the data and apply initial cleaning filters
    SELECT
        member_casual,
        rideable_type,
        start_station_name_clean,
        start_station_id,
        end_station_name_clean,
        end_station_id,

        -- Metric
        COUNT(ride_id) AS total_rides
    FROM
        `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
    WHERE
        -- Filter 1: Exclude rides without defined start/end stations
        start_station_name_clean IS NOT NULL
        AND end_station_name_clean IS NOT NULL
        -- Filter 2: Exclude rides that start and end at the same station (one-way journeys only)
        AND start_station_name_clean <> end_station_name_clean
    GROUP BY 
        1, 2, 3, 4, 5, 6
),


RankedRoutes AS (
    -- Step 2: Rank the aggregated routes within each combination group
    SELECT
        *,
        -- The ROW_NUMBER function partitions (splits) the data by member_casual and rideable_type
        -- and then ranks the routes (rn) within that split based on total_rides (DESC)
        ROW_NUMBER() OVER (
            PARTITION BY member_casual, rideable_type
            ORDER BY total_rides DESC
        ) AS route_rank
    FROM
        AggregatedRoutes
)

-- Step 3: Select all routes where the rank is 100 or less for each group
SELECT
    member_casual,
    rideable_type,
    start_station_name_clean AS start_station_name,
    start_station_id,
    end_station_name_clean AS end_station_name,
    end_station_id,
    total_rides
FROM
    RankedRoutes
WHERE
    route_rank <= 100
ORDER BY
    member_casual DESC, 
    rideable_type ASC,
    total_rides DESC;

 
-- AGGREGATRE ROUTES FOR ROUND TRIP
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.aggregated_round_trip` AS
WITH RoundTripRoutes AS (
    -- Step 1: Group the data by rider type, bike type, and the single station (since start=end)
    SELECT
        member_casual,
        rideable_type,
        start_station_name_clean AS start_station_name,
        start_station_id AS start_station_id,

        -- Metric
        COUNT(ride_id) AS total_round_trips
    FROM
        `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
    WHERE
        -- Filter 1: Exclude rides without defined station names
        start_station_name_clean IS NOT NULL
        -- Filter 2: Select ONLY rides that start and end at the same station (Round Trips)
        AND start_station_name_clean = end_station_name_clean
    GROUP BY 
        1, 2, 3, 4
),

RankedRoundTrips AS (
    -- Step 2: Rank the round-trip stations within each combination group
    SELECT
        *,
        -- Ranks the stations (rn) within the partitions based on total_round_trips (DESC)
        ROW_NUMBER() OVER (
            PARTITION BY member_casual, rideable_type
            ORDER BY total_round_trips DESC
        ) AS station_rank
    FROM
        RoundTripRoutes
)

-- Step 3: Select all stations where the rank is 100 or less for each group
SELECT
    member_casual,
    rideable_type,
    start_station_name,
    start_station_id,
    total_round_trips
FROM
    RankedRoundTrips
WHERE
    station_rank <= 100
ORDER BY
    member_casual DESC, 
    rideable_type ASC,
    total_round_trips DESC;


-- COMPARISION AGGRAGATE FOR ROUND AND ONE WAY TRIPS
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.aggregated_round_oneway_trips` AS
SELECT
    member_casual,
    rideable_type,
    
    -- Count of trips where start station is NOT the same as end station
    COUNTIF(
        start_station_name_clean IS NOT NULL AND end_station_name_clean IS NOT NULL AND 
        start_station_name_clean <> end_station_name_clean
    ) AS total_one_way_trips,
    
    -- Count of trips where start station IS the same as end station
    COUNTIF(
        start_station_name_clean IS NOT NULL AND end_station_name_clean IS NOT NULL AND 
        start_station_name_clean = end_station_name_clean
    ) AS total_round_trips,
    
FROM
    `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
    
GROUP BY 
    member_casual, 
    rideable_type
    
ORDER BY
    member_casual DESC, 
    rideable_type ASC;


-- RAW RIDE COUNT PER STARTING STATION
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.aggreagted_start_station` AS
WITH AggregatedStations AS (
    -- Step 1: Group the data by rider type, bike type, and the starting station
    SELECT
        member_casual,
        rideable_type,
        start_station_name_clean AS start_station_name,
        start_station_id AS start_station_id,

        -- Metric: Raw count of rides starting at this station
        COUNT(ride_id) AS raw_ride_count
    FROM
        `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
    WHERE
        -- Filter: Excludes rides without a defined starting station
        start_station_name_clean IS NOT NULL
    GROUP BY 
        1, 2, 3, 4
),

RankedStations AS (
    -- Step 2: Rank the stations within each combination group
    SELECT
        *,
        -- The ROW_NUMBER function partitions (splits) the data by member_casual and rideable_type
        -- and then ranks the stations (station_rank) within that split based on ride count (DESC)
        ROW_NUMBER() OVER (
            PARTITION BY member_casual, rideable_type
            ORDER BY raw_ride_count DESC
        ) AS station_rank
    FROM
        AggregatedStations
)

-- Step 3: Select all stations where the rank is 100 or less for each group
SELECT
    member_casual,
    rideable_type,
    start_station_name,
    start_station_id,
    raw_ride_count
FROM
    RankedStations
WHERE
    -- This filter isolates the top 100 stations within each of the four groups
    station_rank <= 100
ORDER BY
    member_casual DESC, 
    rideable_type ASC,
    raw_ride_count DESC;


--RAW RIDE COUNT PER ENDING STATION
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.aggregated_end_station` AS
WITH AggregatedStations AS (
    -- Step 1: Group the data by rider type, bike type, and the ENDING station
    SELECT
        member_casual,
        rideable_type,
        end_station_name_clean AS end_station_name,
        end_station_id AS end_station_id,

        -- Metric: Raw count of rides ENDING at this station
        COUNT(ride_id) AS raw_ride_count
    FROM
        `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data`
    WHERE
        -- Filter: Excludes rides without a defined ENDING station
        end_station_name_clean IS NOT NULL
    GROUP BY 
        1, 2, 3, 4
),

RankedStations AS (
    -- Step 2: Rank the stations within each combination group
    SELECT
        *,
        -- Ranks the ENDING stations within the partitions based on ride count (DESC)
        ROW_NUMBER() OVER (
            PARTITION BY member_casual, rideable_type
            ORDER BY raw_ride_count DESC
        ) AS station_rank
    FROM
        AggregatedStations
)

-- Step 3: Select all stations where the rank is 100 or less for each group
SELECT
    member_casual,
    rideable_type,
    end_station_name,
    end_station_id,
    raw_ride_count
FROM
    RankedStations
WHERE
    -- This filter isolates the top 100 stations within each of the four groups
    station_rank <= 100
ORDER BY
    member_casual DESC, 
    rideable_type ASC,
    raw_ride_count DESC;