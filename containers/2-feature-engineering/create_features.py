import pandas as pd
import boto3
import os
import sys
from io import StringIO

def create_lag_features(input_bucket, input_key, output_bucket, output_key):

    print(f"Starting feature engineering job...")
    print(f"Input: s3://{input_bucket}/{input_key}")
    print(f"Output: s3://{output_bucket}/{output_key}")
    
    s3 = boto3.client('s3')
    
    # Download cleaned data from S3
    print("Downloading cleaned data from S3...")
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(obj['Body'], parse_dates=['DATE'])
    
    print(f"Input data shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"\nFirst few rows:\n{df.head()}")
    
    # Ensure data is sorted by date
    df = df.sort_values('DATE').reset_index(drop=True)
    
    # --- Create Lag Features ---
    print("\n--- Creating Lag Features ---")
    
    lag_periods = [7, 14, 21]  # 1 week, 2 weeks, 3 weeks
    weather_vars = ['RAINFALL', 'TMAX', 'TMIN', 'Ave. Temp', 'RH']
    
    # Weather variable lags
    for var in weather_vars:
        if var in df.columns:
            for lag in lag_periods:
                col_name = f"{var}_LAG{lag}"
                df[col_name] = df[var].shift(lag)
                print(f"  Created: {col_name}")
    
    # Dengue case lags (for prediction)
    if 'DENGUE CASES' in df.columns:
        for lag in lag_periods:
            col_name = f"DENGUE_CASES_LAG{lag}"
            df[col_name] = df['DENGUE CASES'].shift(lag)
            print(f"  Created: {col_name}")
    
    # --- Create Rolling Statistics ---
    print("\n--- Creating Rolling Statistics ---")
    
    rolling_windows = [7, 14]  # 1 week, 2 weeks
    
    for var in weather_vars:
        if var in df.columns:
            for window in rolling_windows:
                # Rolling mean
                col_name = f"{var}_ROLL_MEAN_{window}D"
                df[col_name] = df[var].rolling(window=window, min_periods=1).mean()
                print(f"  Created: {col_name}")
                
                # Rolling std (for volatility)
                col_name = f"{var}_ROLL_STD_{window}D"
                df[col_name] = df[var].rolling(window=window, min_periods=1).std()
                print(f"  Created: {col_name}")
    
    # Dengue cases rolling average
    if 'DENGUE CASES' in df.columns:
        for window in rolling_windows:
            col_name = f"DENGUE_CASES_ROLL_MEAN_{window}D"
            df[col_name] = df['DENGUE CASES'].rolling(window=window, min_periods=1).mean()
            print(f"  Created: {col_name}")
    
    # --- Create Interaction Features ---
    print("\n--- Creating Interaction Features ---")
    
    # Temperature range (daily)
    if 'TMAX' in df.columns and 'TMIN' in df.columns:
        df['TEMP_RANGE'] = df['TMAX'] - df['TMIN']
        print("  Created: TEMP_RANGE")
    
    # Rain + Temperature interaction (high rain + high temp = more breeding sites)
    if 'RAINFALL' in df.columns and 'Ave. Temp' in df.columns:
        df['RAIN_TEMP_INTERACTION'] = df['RAINFALL'] * df['Ave. Temp']
        print("  Created: RAIN_TEMP_INTERACTION")
    
    # Humidity + Temperature interaction
    if 'RH' in df.columns and 'Ave. Temp' in df.columns:
        df['HUMIDITY_TEMP_INTERACTION'] = df['RH'] * df['Ave. Temp']
        print("  Created: HUMIDITY_TEMP_INTERACTION")
    
    # --- Create Time-Based Features ---
    print("\n--- Creating Time-Based Features ---")
    
    df['WEEK_OF_YEAR'] = df['DATE'].dt.isocalendar().week
    df['QUARTER'] = df['DATE'].dt.quarter
    df['IS_RAINY_SEASON'] = df['MONTH'].isin([6, 7, 8, 9, 10]).astype(int)  # June-Oct
    
    print("  Created: WEEK_OF_YEAR, QUARTER, IS_RAINY_SEASON")
    
    # --- Summary Statistics ---
    print(f"\n--- Feature Engineering Complete ---")
    print(f"Final data shape: {df.shape}")
    print(f"Total features created: {df.shape[1] - 10} new features")
    print(f"\nMissing values in new features:")
    new_features = [col for col in df.columns if 'LAG' in col or 'ROLL' in col or 'INTERACTION' in col]
    print(df[new_features].isnull().sum().head(20))
    
    print(f"\nFinal dataset preview:\n{df.head()}")
    
    # Upload feature-engineered data to S3
    print(f"\nUploading feature-engineered data to s3://{output_bucket}/{output_key}...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    
    print("✅ Feature engineering job completed successfully!")
    return df.shape[0], df.shape[1]

if __name__ == "__main__":
    # Read environment variables (set by AWS Batch)
    input_bucket = os.environ.get('INPUT_BUCKET')
    input_key = os.environ.get('INPUT_KEY', 'cleaned_manila_weather.csv')
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    output_key = os.environ.get('OUTPUT_KEY', 'features_manila_weather.csv')
    
    if not input_bucket or not output_bucket:
        print("ERROR: INPUT_BUCKET and OUTPUT_BUCKET environment variables must be set")
        sys.exit(1)
    
    try:
        rows, cols = create_lag_features(input_bucket, input_key, output_bucket, output_key)
        print(f"\n🎉 Successfully created {cols} features for {rows} rows")
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)