import pandas as pd
import boto3
import os
import sys
from io import StringIO

def clean_data(input_bucket, input_key, output_bucket, output_key):

    print(f"Starting data cleaning job...")
    print(f"Input: s3://{input_bucket}/{input_key}")
    print(f"Output: s3://{output_bucket}/{output_key}")
    
    s3 = boto3.client('s3')
    
    # Download raw data from S3
    print("Downloading raw data from S3...")
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(obj['Body'])
    
    print(f"Raw data shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"\nFirst few rows:\n{df.head()}")
    print(f"\nMissing values:\n{df.isnull().sum()}")
    
    # Step 1: Remove wind columns
    print("\n--- Step 1: Removing WIND_SPEED and WIND_DIRECTION columns ---")
    df = df.drop(columns=['WIND_SPEED', 'WIND_DIRECTION'], errors='ignore')
    print(f"Columns after removal: {df.columns.tolist()}")
    
    # Step 2: Create DATE column
    print("\n--- Step 2: Creating DATE column ---")
    df['DATE'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY']], errors='coerce')
    df = df.sort_values('DATE').reset_index(drop=True)
    print(f"Date range: {df['DATE'].min()} to {df['DATE'].max()}")
    
    # Step 3: Handle missing values
    print("\n--- Step 3: Handling missing values ---")
    
    # For temperature and humidity: forward fill then backward fill
    temp_humidity_cols = ['TMAX', 'TMIN', 'Ave. Temp', 'RH']
    for col in temp_humidity_cols:
        if col in df.columns:
            missing_before = df[col].isnull().sum()
            df[col] = df[col].ffill().bfill()
            missing_after = df[col].isnull().sum()
            print(f"  {col}: {missing_before} missing → {missing_after} after fill")
    
    # For rainfall: fill with 0
    if 'RAINFALL' in df.columns:
        missing_before = df['RAINFALL'].isnull().sum()
        df['RAINFALL'] = df['RAINFALL'].fillna(0)
        missing_after = df['RAINFALL'].isnull().sum()
        print(f"  RAINFALL: {missing_before} missing → {missing_after} after fill")
    
    # For dengue cases: forward fill (assuming cases persist if not reported)
    if 'DENGUE CASES' in df.columns:
        missing_before = df['DENGUE CASES'].isnull().sum()
        df['DENGUE CASES'] = df['DENGUE CASES'].ffill().fillna(0)
        missing_after = df['DENGUE CASES'].isnull().sum()
        print(f"  DENGUE CASES: {missing_before} missing → {missing_after} after fill")
    
    # Step 4: Remove negative values
    print("\n--- Step 4: Removing negative values ---")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        if col not in ['YEAR', 'MONTH', 'DAY']:  # Keep date fields as-is
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                print(f"  {col}: {negative_count} negative values found, setting to 0")
                df.loc[df[col] < 0, col] = 0
    
    # Step 5: Reorder columns
    col_order = ['DATE', 'YEAR', 'MONTH', 'DAY', 'RAINFALL', 'TMAX', 'TMIN', 
                 'Ave. Temp', 'RH', 'DENGUE CASES']
    df = df[[c for c in col_order if c in df.columns]]
    
    print(f"\n--- Cleaning Complete ---")
    print(f"Final data shape: {df.shape}")
    print(f"Missing values:\n{df.isnull().sum()}")
    print(f"\nCleaned data preview:\n{df.head(10)}")
    
    # Upload cleaned data to S3
    print(f"\nUploading cleaned data to s3://{output_bucket}/{output_key}...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    
    print("✅ Data cleaning job completed successfully!")
    return df.shape[0]

if __name__ == "__main__":
    # Read environment variables (set by AWS Batch)
    input_bucket = os.environ.get('INPUT_BUCKET')
    input_key = os.environ.get('INPUT_KEY', 'sample_manila_weather.csv')
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    output_key = os.environ.get('OUTPUT_KEY', 'cleaned_manila_weather.csv')
    
    if not input_bucket or not output_bucket:
        print("ERROR: INPUT_BUCKET and OUTPUT_BUCKET environment variables must be set")
        sys.exit(1)
    
    try:
        rows_processed = clean_data(input_bucket, input_key, output_bucket, output_key)
        print(f"\n🎉 Successfully processed {rows_processed} rows")
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)