-- ============================================================================
-- Manila Dengue Analysis - Sample Queries
-- ============================================================================

-- Query 1.1: Basic data overview
SELECT 
    COUNT(*) as total_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(DISTINCT year) as num_years,
    COUNT(DISTINCT month) as num_months
FROM dengue_db.weather_features_view;

-- Query 1.2: Check for missing values in key columns
SELECT 
    COUNT(*) as total_rows,
    COUNT(ave_temp) as non_null_temp,
    COUNT(rainfall) as non_null_rainfall,
    COUNT(dengue_cases) as non_null_cases,
    COUNT(ave_temp_lag7) as non_null_lag7
FROM dengue_db.weather_features_view;

-- Query 1.3: View first 10 rows
SELECT * 
FROM dengue_db.weather_features_view
ORDER BY date
LIMIT 10;


-- Query 2.1: Overall summary statistics
SELECT 
    AVG(dengue_cases) as avg_cases,
    STDDEV(dengue_cases) as std_cases,
    MIN(dengue_cases) as min_cases,
    MAX(dengue_cases) as max_cases,
    AVG(ave_temp) as avg_temp,
    AVG(rainfall) as avg_rainfall,
    AVG(rh) as avg_humidity
FROM dengue_db.weather_features_view;

-- Query 2.2: Monthly aggregation
SELECT 
    month,
    COUNT(*) as num_days,
    SUM(dengue_cases) as total_cases,
    AVG(dengue_cases) as avg_daily_cases,
    SUM(rainfall) as total_rainfall,
    AVG(ave_temp) as avg_temperature,
    AVG(rh) as avg_humidity
FROM dengue_db.weather_features_view
GROUP BY month
ORDER BY month;

-- Query 2.3: Seasonal comparison
SELECT 
    season_name,
    COUNT(*) as num_days,
    SUM(dengue_cases) as total_cases,
    AVG(dengue_cases) as avg_daily_cases,
    MIN(dengue_cases) as min_cases,
    MAX(dengue_cases) as max_cases,
    AVG(rainfall) as avg_rainfall,
    AVG(ave_temp) as avg_temperature
FROM dengue_db.weather_features_view
GROUP BY season_name
ORDER BY season_name;


-- Query 3.1: Correlation between weather variables and dengue cases
SELECT 
    CORR(ave_temp, dengue_cases) as temp_correlation,
    CORR(rainfall, dengue_cases) as rainfall_correlation,
    CORR(rh, dengue_cases) as humidity_correlation,
    CORR(temp_range, dengue_cases) as temp_range_correlation
FROM dengue_db.weather_features_view;

-- Query 3.2: Lag feature correlations (7-day)
SELECT 
    CORR(ave_temp_lag7, dengue_cases) as temp_lag7_corr,
    CORR(rainfall_lag7, dengue_cases) as rain_lag7_corr,
    CORR(dengue_cases_lag7, dengue_cases) as cases_lag7_corr
FROM dengue_db.weather_features_view
WHERE ave_temp_lag7 IS NOT NULL;

-- Query 3.3: Compare correlations across different lag periods
SELECT 
    'Current' as lag_period,
    CORR(ave_temp, dengue_cases) as temp_correlation
FROM dengue_db.weather_features_view
UNION ALL
SELECT 
    '7-day lag',
    CORR(ave_temp_lag7, dengue_cases)
FROM dengue_db.weather_features_view
WHERE ave_temp_lag7 IS NOT NULL
UNION ALL
SELECT 
    '14-day lag',
    CORR(ave_temp_lag14, dengue_cases)
FROM dengue_db.weather_features_view
WHERE ave_temp_lag14 IS NOT NULL
UNION ALL
SELECT 
    '21-day lag',
    CORR(ave_temp_lag21, dengue_cases)
FROM dengue_db.weather_features_view
WHERE ave_temp_lag21 IS NOT NULL
ORDER BY lag_period;

-- ============================================================================
-- SECTION 4: RISK ANALYSIS
-- ============================================================================

-- Query 4.1: Risk level distribution
SELECT 
    risk_level,
    COUNT(*) as num_days,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM dengue_db.weather_features_view
GROUP BY risk_level
ORDER BY 
    CASE risk_level
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
    END;

-- Query 4.2: High-risk days analysis
SELECT 
    date,
    dengue_cases,
    ave_temp,
    rainfall,
    rh,
    season_name
FROM dengue_db.weather_features_view
WHERE risk_level = 'High'
ORDER BY dengue_cases DESC;

-- Query 4.3: Risk factors - What conditions lead to high cases?
SELECT 
    risk_level,
    AVG(ave_temp) as avg_temp,
    AVG(rainfall) as avg_rainfall,
    AVG(rh) as avg_humidity,
    AVG(temp_range) as avg_temp_range
FROM dengue_db.weather_features_view
GROUP BY risk_level
ORDER BY 
    CASE risk_level
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
    END;


-- Query 5.1: Weekly trends
SELECT 
    week_of_year,
    AVG(dengue_cases) as avg_cases,
    AVG(ave_temp) as avg_temp,
    SUM(rainfall) as total_rainfall
FROM dengue_db.weather_features_view
GROUP BY week_of_year
ORDER BY week_of_year;

-- Query 5.2: Day of week patterns (if data spans multiple weeks)
SELECT 
    DAYOFWEEK(date) as day_of_week,
    AVG(dengue_cases) as avg_cases,
    COUNT(*) as num_days
FROM dengue_db.weather_features_view
GROUP BY DAYOFWEEK(date)
ORDER BY day_of_week;

-- Query 5.3: Quarterly trends
SELECT 
    quarter,
    COUNT(*) as num_days,
    AVG(dengue_cases) as avg_cases,
    SUM(dengue_cases) as total_cases,
    AVG(rainfall) as avg_rainfall
FROM dengue_db.weather_features_view
GROUP BY quarter
ORDER BY quarter;


-- Query 6.1: Top 10 days with highest cases
SELECT 
    date,
    dengue_cases,
    ave_temp,
    rainfall,
    rh,
    ave_temp_lag7,
    rainfall_lag7,
    season_name
FROM dengue_db.weather_features_view
ORDER BY dengue_cases DESC
LIMIT 10;

-- Query 6.2: Days with perfect storm (high temp + high rainfall)
SELECT 
    date,
    dengue_cases,
    ave_temp,
    rainfall,
    rain_temp_interaction,
    risk_level
FROM dengue_db.weather_features_view
WHERE ave_temp > 28 AND rainfall > 5
ORDER BY dengue_cases DESC;

-- Query 6.3: Rolling average effectiveness
SELECT 
    AVG(ABS(dengue_cases - dengue_cases_roll_mean_7d)) as mae_7d,
    AVG(ABS(dengue_cases - dengue_cases_roll_mean_14d)) as mae_14d
FROM dengue_db.weather_features_view
WHERE dengue_cases_roll_mean_7d IS NOT NULL;



-- Query 7.1: Export monthly summary for visualization
SELECT 
    year,
    month,
    SUM(dengue_cases) as total_cases,
    AVG(dengue_cases) as avg_cases,
    SUM(rainfall) as total_rainfall,
    AVG(ave_temp) as avg_temp,
    AVG(rh) as avg_humidity,
    COUNT(CASE WHEN risk_level = 'High' THEN 1 END) as high_risk_days
FROM dengue_db.weather_features_view
GROUP BY year, month
ORDER BY year, month;

-- Query 7.2: Export dataset with all features for ML modeling
SELECT *
FROM dengue_db.weather_features_view
WHERE ave_temp_lag7 IS NOT NULL  -- Only include rows with lag features
ORDER BY date;

-- Query 7.3: Export correlation matrix data
SELECT 
    'ave_temp' as feature,
    CORR(ave_temp, dengue_cases) as correlation
FROM dengue_db.weather_features_view
UNION ALL
SELECT 'rainfall', CORR(rainfall, dengue_cases)
FROM dengue_db.weather_features_view
UNION ALL
SELECT 'rh', CORR(rh, dengue_cases)
FROM dengue_db.weather_features_view
UNION ALL
SELECT 'temp_range', CORR(temp_range, dengue_cases)
FROM dengue_db.weather_features_view
ORDER BY correlation DESC;

