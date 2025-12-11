import pandas as pd
import boto3
import os
import sys
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for AWS
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO, StringIO
import warnings
warnings.filterwarnings('ignore')

def generate_eda(input_bucket, input_key, output_bucket, output_prefix):

    print(f"Starting EDA job...")
    print(f"Input: s3://{input_bucket}/{input_key}")
    print(f"Output: s3://{output_bucket}/{output_prefix}*.png/csv")
    
    s3 = boto3.client('s3')
    
    # Download feature-engineered data from S3
    print("Downloading feature data from S3...")
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(obj['Body'], parse_dates=['DATE'])
    
    print(f"Input data shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 8)
    
    # --- 1. Generate Summary Statistics ---
    print("\n--- Generating Summary Statistics ---")
    
    summary = df.describe().T
    summary['missing'] = df.isnull().sum()
    summary['missing_pct'] = (df.isnull().sum() / len(df) * 100).round(2)
    
    print(summary)
    
    # Save summary statistics
    csv_buffer = StringIO()
    summary.to_csv(csv_buffer)
    s3.put_object(
        Bucket=output_bucket, 
        Key=f"{output_prefix}summary_statistics.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"✅ Uploaded: {output_prefix}summary_statistics.csv")
    
    # --- 2. Correlation Heatmap ---
    print("\n--- Generating Correlation Heatmap ---")
    
    # Select numeric columns (exclude date columns)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in ['YEAR', 'MONTH', 'DAY', 'WEEK_OF_YEAR', 'QUARTER']]
    
    # Limit to top 20 features for readability
    if len(numeric_cols) > 20:
        # Prioritize original features and dengue-related features
        priority_cols = [col for col in numeric_cols if 'DENGUE' in col or 'LAG' not in col]
        other_cols = [col for col in numeric_cols if col not in priority_cols]
        numeric_cols = priority_cols + other_cols[:max(0, 20-len(priority_cols))]
    
    corr_matrix = df[numeric_cols].corr()
    
    plt.figure(figsize=(14, 12))
    sns.heatmap(corr_matrix, annot=False, cmap='coolwarm', center=0, 
                square=True, linewidths=0.5, cbar_kws={"shrink": 0.8})
    plt.title('Correlation Heatmap - Manila Weather & Dengue Data', fontsize=16, pad=20)
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    s3.put_object(
        Bucket=output_bucket,
        Key=f"{output_prefix}correlation_heatmap.png",
        Body=img_buffer.getvalue(),
        ContentType='image/png'
    )
    plt.close()
    print(f"✅ Uploaded: {output_prefix}correlation_heatmap.png")
    
    # --- 3. Time Series Plots ---
    print("\n--- Generating Time Series Plots ---")
    
    fig, axes = plt.subplots(3, 2, figsize=(16, 12))
    fig.suptitle('Manila Weather & Dengue Cases Over Time', fontsize=18, y=0.995)
    
    # Plot 1: Dengue cases
    axes[0, 0].plot(df['DATE'], df['DENGUE CASES'], color='red', linewidth=1.5)
    axes[0, 0].set_title('Dengue Cases', fontsize=12)
    axes[0, 0].set_ylabel('Cases')
    axes[0, 0].grid(True, alpha=0.3)
    
    # Plot 2: Rainfall
    axes[0, 1].bar(df['DATE'], df['RAINFALL'], color='blue', alpha=0.6, width=1)
    axes[0, 1].set_title('Rainfall', fontsize=12)
    axes[0, 1].set_ylabel('mm')
    axes[0, 1].grid(True, alpha=0.3)
    
    # Plot 3: Temperature (Max, Min, Average)
    axes[1, 0].plot(df['DATE'], df['TMAX'], label='Max Temp', color='orange', linewidth=1)
    axes[1, 0].plot(df['DATE'], df['TMIN'], label='Min Temp', color='blue', linewidth=1)
    axes[1, 0].plot(df['DATE'], df['Ave. Temp'], label='Avg Temp', color='green', linewidth=1.5)
    axes[1, 0].set_title('Temperature', fontsize=12)
    axes[1, 0].set_ylabel('°C')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # Plot 4: Relative Humidity
    axes[1, 1].plot(df['DATE'], df['RH'], color='purple', linewidth=1.5)
    axes[1, 1].set_title('Relative Humidity', fontsize=12)
    axes[1, 1].set_ylabel('%')
    axes[1, 1].grid(True, alpha=0.3)
    
    # Plot 5: Dengue vs 7-day avg temp
    if 'Ave. Temp_ROLL_MEAN_7D' in df.columns:
        ax5 = axes[2, 0]
        ax5.plot(df['DATE'], df['Ave. Temp_ROLL_MEAN_7D'], color='green', linewidth=1.5)
        ax5.set_ylabel('7-Day Avg Temp (°C)', color='green')
        ax5.tick_params(axis='y', labelcolor='green')
        
        ax5_twin = ax5.twinx()
        ax5_twin.plot(df['DATE'], df['DENGUE CASES'], color='red', linewidth=1.5, alpha=0.7)
        ax5_twin.set_ylabel('Dengue Cases', color='red')
        ax5_twin.tick_params(axis='y', labelcolor='red')
        
        ax5.set_title('Temperature vs Dengue Cases', fontsize=12)
        ax5.grid(True, alpha=0.3)
    
    # Plot 6: Rainfall vs Dengue
    ax6 = axes[2, 1]
    ax6.bar(df['DATE'], df['RAINFALL'], color='blue', alpha=0.4, width=1)
    ax6.set_ylabel('Rainfall (mm)', color='blue')
    ax6.tick_params(axis='y', labelcolor='blue')
    
    ax6_twin = ax6.twinx()
    ax6_twin.plot(df['DATE'], df['DENGUE CASES'], color='red', linewidth=1.5)
    ax6_twin.set_ylabel('Dengue Cases', color='red')
    ax6_twin.tick_params(axis='y', labelcolor='red')
    
    ax6.set_title('Rainfall vs Dengue Cases', fontsize=12)
    ax6.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    s3.put_object(
        Bucket=output_bucket,
        Key=f"{output_prefix}timeseries_plots.png",
        Body=img_buffer.getvalue(),
        ContentType='image/png'
    )
    plt.close()
    print(f"✅ Uploaded: {output_prefix}timeseries_plots.png")
    
    # --- 4. Distribution Plots ---
    print("\n--- Generating Distribution Plots ---")
    
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    fig.suptitle('Distribution of Key Variables', fontsize=18, y=0.995)
    
    vars_to_plot = ['DENGUE CASES', 'RAINFALL', 'Ave. Temp', 'RH', 'TMAX', 'TMIN']
    
    for i, var in enumerate(vars_to_plot):
        row = i // 3
        col = i % 3
        
        axes[row, col].hist(df[var].dropna(), bins=30, color='steelblue', alpha=0.7, edgecolor='black')
        axes[row, col].set_title(var, fontsize=12)
        axes[row, col].set_xlabel('Value')
        axes[row, col].set_ylabel('Frequency')
        axes[row, col].grid(True, alpha=0.3)
        
        # Add mean line
        mean_val = df[var].mean()
        axes[row, col].axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_val:.2f}')
        axes[row, col].legend()
    
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    s3.put_object(
        Bucket=output_bucket,
        Key=f"{output_prefix}distributions.png",
        Body=img_buffer.getvalue(),
        ContentType='image/png'
    )
    plt.close()
    print(f"✅ Uploaded: {output_prefix}distributions.png")
    
    # --- 5. Monthly Aggregations ---
    print("\n--- Generating Monthly Aggregation Plot ---")
    
    df['YEAR_MONTH'] = df['DATE'].dt.to_period('M')
    monthly_agg = df.groupby('YEAR_MONTH').agg({
        'DENGUE CASES': 'sum',
        'RAINFALL': 'sum',
        'Ave. Temp': 'mean',
        'RH': 'mean'
    }).reset_index()
    monthly_agg['YEAR_MONTH'] = monthly_agg['YEAR_MONTH'].dt.to_timestamp()
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Monthly Aggregated Metrics', fontsize=18, y=0.995)
    
    axes[0, 0].bar(monthly_agg['YEAR_MONTH'], monthly_agg['DENGUE CASES'], color='red', alpha=0.7)
    axes[0, 0].set_title('Total Monthly Dengue Cases')
    axes[0, 0].set_ylabel('Cases')
    axes[0, 0].grid(True, alpha=0.3)
    
    axes[0, 1].bar(monthly_agg['YEAR_MONTH'], monthly_agg['RAINFALL'], color='blue', alpha=0.7)
    axes[0, 1].set_title('Total Monthly Rainfall')
    axes[0, 1].set_ylabel('mm')
    axes[0, 1].grid(True, alpha=0.3)
    
    axes[1, 0].plot(monthly_agg['YEAR_MONTH'], monthly_agg['Ave. Temp'], marker='o', color='green', linewidth=2)
    axes[1, 0].set_title('Average Monthly Temperature')
    axes[1, 0].set_ylabel('°C')
    axes[1, 0].grid(True, alpha=0.3)
    
    axes[1, 1].plot(monthly_agg['YEAR_MONTH'], monthly_agg['RH'], marker='o', color='purple', linewidth=2)
    axes[1, 1].set_title('Average Monthly Humidity')
    axes[1, 1].set_ylabel('%')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    s3.put_object(
        Bucket=output_bucket,
        Key=f"{output_prefix}monthly_aggregations.png",
        Body=img_buffer.getvalue(),
        ContentType='image/png'
    )
    plt.close()
    print(f"✅ Uploaded: {output_prefix}monthly_aggregations.png")
    
    print("\n✅ EDA job completed successfully!")
    return 4  # Number of visualizations created

if __name__ == "__main__":
    # Read environment variables
    input_bucket = os.environ.get('INPUT_BUCKET')
    input_key = os.environ.get('INPUT_KEY', 'features_manila_weather.csv')
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    output_prefix = os.environ.get('OUTPUT_PREFIX', 'eda/')
    
    # Ensure output_prefix ends with /
    if not output_prefix.endswith('/'):
        output_prefix += '/'
    
    if not input_bucket or not output_bucket:
        print("ERROR: INPUT_BUCKET and OUTPUT_BUCKET environment variables must be set")
        sys.exit(1)
    
    try:
        num_plots = generate_eda(input_bucket, input_key, output_bucket, output_prefix)
        print(f"\n🎉 Successfully generated {num_plots} visualizations + summary stats")
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)