--SELECT QUERY USING A CTE TO CHECK FOR CONSISTENCY AND INTEGRITY IN KEY DATA
WITH Consolidated_Raw_Data AS (
  -- 1. UNION ALL all 12 months to cover full seasonality
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.11_2024_divvy_tripdata` 
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.12_2024_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.01_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.02_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.03_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.04_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.05_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.06_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.07_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.08_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.09_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.10_2025_divvy_tripdata`
)


SELECT
  -- 1. RIDE_ID CHECK
  (
    SELECT COUNT(ride_id) - COUNT(DISTINCT ride_id)
    FROM Consolidated_Raw_Data
  ) AS ride_id_duplicates_count,
  
  COUNTIF(t.ride_id IS NULL) AS ride_id_null_count,
  
  COUNTIF(TRIM(t.ride_id) = '') AS ride_id_empty_string_count,
  
  -- 2. RIDEABLE_TYPE CHECK
  COUNTIF(t.rideable_type IS NULL) AS rideable_type_null_count,
  
  COUNTIF(TRIM(t.rideable_type) = '') AS rideable_type_empty_string_count,
  
  (
    SELECT COUNT(DISTINCT rideable_type)
    FROM Consolidated_Raw_Data
  ) AS rideable_type_unique_count,
  
  -- Use STRING_AGG to list all unique types found, in case there are more than 2
  (
    SELECT STRING_AGG(DISTINCT rideable_type, ', ' ORDER BY rideable_type)
    FROM Consolidated_Raw_Data
  ) AS rideable_type_unique_values,
  
  -- 3. DATETIME CHECK
  COUNTIF(t.started_at IS NULL) AS started_at_null_count,
  
  -- Rely on the column being correctly typed as DATETIME. If it were a STRING, 
  -- Check for TRIM(t.started_at) = ''
  
  COUNTIF(t.ended_at IS NULL) AS ended_at_null_count,
  
  -- CRITICAL CHECK: Trips where the end time is before the start time
  COUNTIF(t.ended_at <= t.started_at) AS invalid_trip_duration_count,
  
  -- TOTAL ROWS (for context)
  COUNT(1) AS total_records_checked

FROM
  Consolidated_Raw_Data AS t;



-- SELECT QUERY USING A CTE TO TEST ALL FEATURE ENGINEERING AND CLEANING LOGIC ON A SINGLE MONTH OF DATA

-- CTE: Handles all data cleaning, standardization, and feature calculation.
WITH Feature_Engineering AS (
    SELECT 
        raw.ride_id,
        raw.rideable_type,
        raw.member_casual,
        raw.started_at,
        raw.ended_at,

        -- 1. CLEANED STATION FIELDS (Retaining NULLs, Cleaning where data exists)
        -- Logic: Remove (TEMP), TRIM, Convert empty strings to NULL, then apply Title Case (INITCAP).
        INITCAP(TRIM(REPLACE(NULLIF(TRIM(raw.start_station_name), ''), ' (TEMP)', ''))) AS start_station_name_clean,
        raw.start_station_id,
        INITCAP(TRIM(REPLACE(NULLIF(TRIM(raw.end_station_name), ''), ' (TEMP)', ''))) AS end_station_name_clean,
        raw.end_station_id,

        -- 2. DURATION FEATURES
        TIMESTAMP_DIFF(raw.ended_at, raw.started_at, MINUTE) AS ride_length_minutes,
        ROUND(TIMESTAMP_DIFF(raw.ended_at, raw.started_at, MINUTE) / 60, 2) AS ride_length_hours,

        -- 3. TEMPORAL FEATURES
        FORMAT_DATE('%A', raw.started_at) AS day_of_week_name,
        FORMAT_DATE('%B', raw.started_at) AS trip_month_name,
        FORMAT_DATE('%Y', raw.started_at) AS trip_year,
        EXTRACT(HOUR FROM raw.started_at) AS trip_hour,
        
        -- 4. CATEGORICAL TEMPORAL FEATURES
        -- Classify Day Type (Weekday vs. Weekend)
        CASE EXTRACT(DAYOFWEEK FROM raw.started_at)
          WHEN 1 THEN 'Weekend' -- 1 is Sunday
          WHEN 7 THEN 'Weekend' -- 7 is Saturday
          ELSE 'Weekday'
        END AS day_type,
        
        -- Categorize Time of Day (Morning, Afternoon, Evening, Night)
        CASE 
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 5 AND 11 THEN 'Morning'
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 12 AND 16 THEN 'Afternoon'
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 17 AND 20 THEN 'Evening'
          ELSE 'Night'
        END AS time_of_day_category

    FROM
        `portfolio-projects-09.cyclistic_dataset.11_2024_divvy_tripdata` AS raw -- Using 'raw' alias for clarity
)
-- Final SELECT: Applies the main data quality filter and ordering
SELECT 
    * FROM
    Feature_Engineering
WHERE
    -- Data Quality Filter: Exclude short trips under 1 min and trips with end time earlier than start time
    ride_length_minutes >= 1
    -- Exclude bad member_casual values
    AND member_casual IN ('member', 'casual')
ORDER BY
    ride_length_minutes,
    trip_hour;


/*
    FINAL QUERY TO PROCESS ALL TABLES FOR ANALYSIS
    USING CREATE OR REPLACE TABLE to PHYSICALLY SAVE THE CLEANED DATA
    THE NEW TABLE CONTAINS NEW FEATURES TO SUPPORT THE ANALYZA PHASE
*/

CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data` AS
WITH Consolidated_Raw_Data AS (
  -- 1. UNION ALL all 12 months to cover full seasonality
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.11_2024_divvy_tripdata` 
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.12_2024_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.01_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.02_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.03_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.04_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.05_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.06_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.07_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.08_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.09_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.10_2025_divvy_tripdata`
),

-- CTE: Handles all data cleaning, standardization, and feature calculation
Feature_Engineering AS (
    SELECT 
        raw.ride_id,
        raw.rideable_type,
        raw.member_casual,
        raw.started_at,
        raw.ended_at,

        -- 1. CLEANED STATION FIELDS (Retaining NULLs, Cleaning where data exists)
        -- Logic: Remove (TEMP), TRIM, Convert empty strings to NULL, then apply Title Case (INITCAP)
        INITCAP(TRIM(REPLACE(NULLIF(TRIM(raw.start_station_name), ''), ' (TEMP)', ''))) AS start_station_name_clean,
        raw.start_station_id,
        INITCAP(TRIM(REPLACE(NULLIF(TRIM(raw.end_station_name), ''), ' (TEMP)', ''))) AS end_station_name_clean,
        raw.end_station_id,

        -- 2. DURATION FEATURES
        -- BigQuery TIMESTAMP_DIFF returns INT64, which is ideal for ride length
        TIMESTAMP_DIFF(raw.ended_at, raw.started_at, MINUTE) AS ride_length_minutes,
        ROUND(TIMESTAMP_DIFF(raw.ended_at, raw.started_at, MINUTE) / 60, 2) AS ride_length_hours,

        -- 3. TEMPORAL FEATURES
        FORMAT_DATE('%A', raw.started_at) AS day_of_week_name,
        FORMAT_DATE('%b - %Y', raw.started_at) AS trip_month_year,
        EXTRACT(HOUR FROM raw.started_at) AS trip_hour,
        
        -- 4. CATEGORICAL TEMPORAL FEATURES
        -- Classify Day Type (Weekday vs. Weekend)
        -- BigQuery: 1=Sunday, 2=Monday, ..., 7=Saturday
        CASE EXTRACT(DAYOFWEEK FROM raw.started_at)
          WHEN 1 THEN 'Weekend' -- Sunday
          WHEN 7 THEN 'Weekend' -- Saturday
          ELSE 'Weekday'
        END AS day_type,
        
        -- Categorize Time of Day (Morning, Afternoon, Evening, Night)
        CASE 
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 5 AND 11 THEN 'Morning'
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 12 AND 16 THEN 'Afternoon'
          WHEN EXTRACT(HOUR FROM raw.started_at) BETWEEN 17 AND 20 THEN 'Evening'
          ELSE 'Night'
        END AS time_of_day_category

    FROM
        Consolidated_Raw_Data AS raw -- FIX: Changed source to use the 12-month CTE
)
-- Final SELECT: Applies the main data quality filter and ordering
SELECT 
    * FROM
    Feature_Engineering
WHERE
    -- Data Quality Filter (Addresses requirements from previous chat):
    
    -- 1. RIDE ID: Must not be null or an empty string.
    ride_id IS NOT NULL AND TRIM(ride_id) != ''

    -- 2. RIDEABLE TYPE: Must be one of the two expected types (assuming two types: 'classic_bike' and 'electric_bike')
    -- No match case for the conditon in this dataset 
    -- However beneficial for future cases
    AND rideable_type IN ('classic_bike', 'electric_bike')
    
    -- 3. DATETIME: Check for nulls/invalid logic
    AND started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND ended_at > started_at -- Exclude trips where the end time is before the start time
    
    -- 4. BUSINESS LOGIC FILTER: Exclude extremely short trips (noise)
    AND ride_length_minutes BETWEEN 1 AND 1440

    -- 5. MEMBERSHIP FILTER: Exclude bad member_casual values
    -- No match case for the conditon in this dataset 
    -- However beneficial for future cases
    AND member_casual IN ('member', 'casual')
ORDER BY
    ride_length_minutes,
    trip_hour;



/*
  OVERVIEW OF THE NUMBERS OF ROWS DROPPED BY EACH SPECIFIC FILTER
  APPLIED IN THE FINAL FEATURE ENGINEERING QUERY
*/


WITH Consolidated_Raw_Data AS (
  -- 1. UNION ALL all 12 months to cover full seasonality
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.11_2024_divvy_tripdata` 
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.12_2024_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.01_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.02_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.03_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.04_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.05_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.06_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.07_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.08_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.09_2025_divvy_tripdata`
    UNION ALL
  SELECT * FROM `portfolio-projects-09.cyclistic_dataset.10_2025_divvy_tripdata`
)

SELECT
  -- Total Records for context
  COUNT(t.ride_id) AS total_initial_records,
  
  -- DROPPED BY RIDE ID (Filter 1)
  -- Filter: ride_id IS NOT NULL AND TRIM(ride_id) != ''
  COUNTIF(t.ride_id IS NULL OR TRIM(t.ride_id) = '') 
    AS dropped_by_ride_id_missing,
  
  -- DROPPED BY RIDEABLE TYPE (Filter 2)
  -- Filter: rideable_type IN ('classic_bike', 'electric_bike')
  -- Note: This includes NULLs and empty strings that are not the two expected types.
  COUNTIF(t.rideable_type NOT IN ('classic_bike', 'electric_bike')) 
    AS dropped_by_rideable_type_unexpected,

  -- DROPPED BY DATETIME NULLS (Filters 3 & 4)
  -- Filter: started_at IS NOT NULL AND ended_at IS NOT NULL
  COUNTIF(t.started_at IS NULL OR t.ended_at IS NULL) 
    AS dropped_by_datetime_nulls,

  -- DROPPED BY INVALID DURATION (Filter 5)
  -- Filter: ended_at > started_at 
  -- Note: This only counts cases where both are non-null and the end time is invalid/before start.
  COUNTIF(t.ended_at <= t.started_at) 
    AS dropped_by_invalid_duration,

  -- DROPPED BY SHORT AND LONG TRIPS (Filter 6)
  -- Filter: ride_length_minutes BETWEEN 1 AND 1440
  COUNTIF(TIMESTAMP_DIFF(t.ended_at, t.started_at, MINUTE) < 1) 
    AS dropped_by_short_duration_lt_1_min,

  COUNTIF(TIMESTAMP_DIFF(t.ended_at, t.started_at, MINUTE) > 1440) 
    AS dropped_by_long_duration_lt_24_hours,
  
  -- DROPPED BY MEMBERSHIP TYPE (Filter 7)
  -- Filter: member_casual IN ('member', 'casual')
  -- Note: This counts NULLs, empty strings, and any other unexpected values.
  COUNTIF(t.member_casual NOT IN ('member', 'casual')) 
    AS dropped_by_invalid_member_type

FROM
  Consolidated_Raw_Data AS t;


--EXCLUDING IRRELEVANT TIME PERIOD (OCT - 2024) FROM FINAL PROCESSED DATASET
CREATE OR REPLACE TABLE `portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data` AS
  SELECT
    *
  FROM
    portfolio-projects-09.cyclistic_dataset.cleaned_analysis_data
  WHERE
    trip_month_year <> "Oct - 2024"