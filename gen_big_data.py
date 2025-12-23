import polars as pl
import numpy as np
from datetime import datetime, timedelta
import os

def generate_million_rows():
    source_path = "exports/Master_PPC_Data.parquet"
    output_path = "exports/Big_Master_PPC_Data.parquet"
    
    print(f"Loading seed data from {source_path}...")
    try:
        seed_df = pl.read_parquet(source_path)
    except Exception:
        print("⚠️ Không tìm thấy file gốc. Sẽ dùng data giả lập hoàn toàn.")
        seed_df = pl.DataFrame({
            "SKU": ["SKU-A", "SKU-B"],
            "ASIN": ["B001", "B002"],
            "Product Name": ["Prod A", "Prod B"],
            "Revenue (Actual)": [5000.0, 3000.0],
            "Ads Spend (Actual)": [1000.0, 800.0],
            "Unit sold (Actual)": [100, 50],
            "ROAS": [5.0, 3.75]
        })

    # 1. Analyze Seed Data
    unique_skus = seed_df["SKU"].unique().to_list()
    if len(unique_skus) < 1: unique_skus = ["SKU-GEN-01"]
    
    # Calculate stats to mimic distribution
    avg_rev = seed_df["Revenue (Actual)"].mean() or 2000
    std_rev = seed_df["Revenue (Actual)"].std() or 500
    
    print(f"Seed Analysis: Found {len(unique_skus)} SKUs. Avg Revenue: {avg_rev:.2f}")

    # 2. Configuration for 1 Million Rows
    # Target: ~1,000,000 rows
    # Date Range: 2024-01-01 to 2025-11-30 (~700 days)
    # Needed SKUs = 1,000,000 / 700 = ~1430 SKUs.
    
    TARGET_SKUS = 1500
    START_DATE = datetime(2024, 1, 1)
    END_DATE = datetime(2025, 11, 30)
    days = (END_DATE - START_DATE).days + 1
    
    print(f"🚀 Generaton Plan: {TARGET_SKUS} SKUs x {days} Days = {TARGET_SKUS * days:,} rows.")

    # 3. Generate Mock SKUs
    print("Generating SKU list...")
    mock_skus = []
    mock_asins = []
    mock_names = []
    
    # Cycle through existing SKUs to create variations
    for i in range(TARGET_SKUS):
        base_sku = unique_skus[i % len(unique_skus)]
        suffix = f"{i:04d}"
        mock_skus.append(f"{base_sku}_V{suffix}")
        mock_asins.append(f"ASIN_{suffix}")
        mock_names.append(f"Product {base_sku} Variant {suffix}")

    # 4. Generate Date Range
    date_range = [START_DATE + timedelta(days=x) for x in range(days)]
    date_strs = [d.strftime("%Y-%m-%d") for d in date_range]

    # 5. Build Big DataFrame (Using Cartesian Product Strategy for Speed)
    # Create DataFrame of SKUs
    df_skus = pl.DataFrame({
        "SKU": mock_skus, 
        "ASIN": mock_asins, 
        "Product Name": mock_names,
        # Assign random "base performance" per SKU so they don't look identical
        "Base_Rev": np.random.normal(avg_rev, std_rev, size=TARGET_SKUS).clip(min=100) 
    })
    
    # Create DataFrame of Dates
    df_dates = pl.DataFrame({"Report_Date": date_strs})
    
    print("Performing Cross Join (This might take a moment)...")
    # Cross join to get SKU x Date grid
    big_df = df_skus.join(df_dates, how="cross")
    
    total_rows = len(big_df)
    print(f"Grid created: {total_rows:,} rows. Filling metrics...")

    # 6. Fill Metrics with Numpy (Vectorized for speed)
    # Add random daily fluctuation (noise)
    noise = np.random.uniform(0.5, 1.5, size=total_rows)
    seasonality = np.random.uniform(0.8, 1.2, size=total_rows) # Simplistic seasonality
    
    revenue = big_df["Base_Rev"].to_numpy() * noise * seasonality
    
    # ACOS logic: Spend is usually a % of revenue + randomness
    acos_base = np.random.uniform(0.1, 0.6, size=total_rows) # 10% to 60% ACOS
    spend = revenue * acos_base
    
    # Units logic: Price varies slightly
    avg_price = np.random.uniform(20, 50, size=total_rows)
    units = (revenue / avg_price).astype(int)
    
    # ROAS
    roas = np.divide(revenue, spend, out=np.zeros_like(revenue), where=spend!=0)
    
    # Phases (Randomly assign)
    phases = np.random.choice([1, 2, 3], size=total_rows, p=[0.1, 0.3, 0.6])

    # Assign columns back to Polars
    big_df = big_df.with_columns([
        pl.Series("Revenue (Actual)", revenue).round(2),
        pl.Series("Ads Spend (Actual)", spend).round(2),
        pl.Series("Unit sold (Actual)", units),
        pl.Series("ROAS", roas).round(2),
        pl.Series("Phase", phases),
        pl.lit(0.0).alias("Refund"),
        pl.lit(100).alias("FBA Stock"), # Mock constant stock
        pl.col("Report_Date").alias("Date_Start"),
        pl.col("Report_Date").alias("Date_End")
    ])

    # Drop temp column
    big_df = big_df.drop("Base_Rev")

    # 7. Save
    print(f"Saving to {output_path}...")
    big_df.write_parquet(output_path, compression="zstd")
    
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"✅ DONE! Generated {total_rows:,} rows.")
    print(f"📁 Output Size: {file_size:.2f} MB")
    print(f"👉 Use this path in your App: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    generate_million_rows()
