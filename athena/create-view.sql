-- ============================================================================
-- Manila Dengue Weather Features - View Creation
-- ============================================================================

-- Drop view if exists (for clean recreation)
DROP VIEW IF EXISTS dengue_db.weather_features_view;

-- Create comprehensive view with clean column names and calculated fields
CREATE OR REPLACE VIEW dengue_db.weather_features_view AS
SELECT 
    -- Convert date string to proper DATE type
    CAST(date_col AS DATE) as date,
    
    -- Original temporal columns
    year,
    month,
    day,
    
    -- Core weather metrics
    rainfall,
    tmax,
    tmin,
    ave_temp,
    rh,
    dengue_cases,
    
    -- Lag features (7 days)
    rainfall_lag7,
    tmax_lag7,
    tmin_lag7,
    ave_temp_lag7,
    rh_lag7,
    dengue_cases_lag7,
    
    -- Lag features (14 days)
    rainfall_lag14,
    tmax_lag14,
    tmin_lag14,
    ave_temp_lag14,
    rh_lag14,
    dengue_cases_lag14,
    
    -- Lag features (21 days)
    rainfall_lag21,
    tmax_lag21,
    tmin_lag21,
    ave_temp_lag21,
    rh_lag21,
    dengue_cases_lag21,
    
    -- Rolling statistics (7-day window)
    rainfall_roll_mean_7d,
    rainfall_roll_std_7d,
    tmax_roll_mean_7d,
    tmax_roll_std_7d,
    tmin_roll_mean_7d,
    tmin_roll_std_7d,
    ave_temp_roll_mean_7d,
    ave_temp_roll_std_7d,
    rh_roll_mean_7d,
    rh_roll_std_7d,
    dengue_cases_roll_mean_7d,
    
    -- Rolling statistics (14-day window)
    rainfall_roll_mean_14d,
    rainfall_roll_std_14d,
    tmax_roll_mean_14d,
    tmax_roll_std_14d,
    tmin_roll_mean_14d,
    tmin_roll_std_14d,
    ave_temp_roll_mean_14d,
    ave_temp_roll_std_14d,
    rh_roll_mean_14d,
    rh_roll_std_14d,
    dengue_cases_roll_mean_14d,
    
    -- Interaction features
    temp_range,
    rain_temp_interaction,
    humidity_temp_interaction,
    
    -- Temporal features
    week_of_year,
    quarter,
    is_rainy_season,
    
    -- Calculated field: Season name (readable)
    CASE 
        WHEN is_rainy_season = 1 THEN 'Rainy' 
        ELSE 'Dry' 
    END as season_name,
    
    -- Calculated field: Risk level classification
    CASE 
        WHEN dengue_cases >= 70 THEN 'High'
        WHEN dengue_cases >= 40 THEN 'Medium'
        ELSE 'Low'
    END as risk_level,
    
    -- Calculated field: Check if lag data is available
    CASE 
        WHEN ave_temp_lag7 IS NOT NULL THEN 'Available'
        ELSE 'Not Available'
    END as lag_data_status,
    
    -- Calculated field: Days since start of dataset
    DATEDIFF(day, 
             (SELECT MIN(CAST(date_col AS DATE)) FROM dengue_db.manila_weather_features), 
             CAST(date_col AS DATE)
    ) as days_from_start
    
FROM dengue_db.manila_weather_features;

-- Step 2: Verify view creation and data
SELECT 
    COUNT(*) as total_rows,
    MIN(date) as start_date,
    MAX(date) as end_date,
    COUNT(DISTINCT season_name) as num_seasons,
    COUNT(DISTINCT risk_level) as num_risk_levels
FROM dengue_db.weather_features_view;

-- Expected output: Summary statistics showing data range and categories