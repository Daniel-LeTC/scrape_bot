import argparse
import calendar
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import fastexcel  # Import trực tiếp
import polars as pl
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import config

# Load environment variables from .env file
load_dotenv()

class AutoLogin:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.login_url = config.LOGIN_URL
        self.target_url = config.DASHBOARD_URL

    def get_token(self):
        """
        Automates login process using Playwright to retrieve authentication token.
        Returns the token string if successful, None otherwise.
        """
        if not self.username or not self.password:
            print("Error: PPC_USER or PPC_PASS not found in environment variables.")
            return None

        print(f"Attempting login for user: {self.username}...")
        
        with sync_playwright() as p:
            browser = None
            try:
                # Launch browser in headless mode with stealth arguments
                browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"]
                )
                
                # Create context with a standard user agent
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()

                # Navigate to login page and wait for network to settle
                page.goto(self.login_url, wait_until="networkidle")
                
                # Extra wait for JS/Hydration
                page.wait_for_timeout(2000)

                # Wait for specific IDs provided by user
                try:
                    page.wait_for_selector('#username', timeout=15000)
                except Exception:
                    # Capture screenshot on failure for debugging
                    debug_path = os.path.join(config.OUTPUT_DIR, "debug_login_error.png")
                    page.screenshot(path=debug_path)
                    print(f"Error: Timeout waiting for login fields. Screenshot saved to {debug_path}")
                    return None
                
                # Input credentials using exact IDs
                page.fill('#username', self.username)
                page.fill('#password', self.password)
                
                # Submit form using type="submit"
                page.click('button[type="submit"]')
                
                # Wait for navigation away from login page
                print("Waiting for redirect...")
                # Wait until URL does NOT contain 'signin' anymore
                page.wait_for_function("!window.location.href.includes('signin')", timeout=30000)
                
                # Wait a bit for LocalStorage to be populated
                page.wait_for_timeout(2000)
                print("Login successful (Redirected).")

                # Retrieve token from Local Storage
                storage = page.evaluate("() => JSON.stringify(window.localStorage)")
                storage_dict = json.loads(storage)
                
                token = storage_dict.get("access_token") or \
                        storage_dict.get("token") or \
                        storage_dict.get("auth_token")
                
                # Fallback: Retrieve token from Cookies
                if not token:
                    cookies = context.cookies()
                    for c in cookies:
                        if "token" in c['name'].lower():
                            token = c['value']
                            break
                
                return token
            except Exception as e:
                print(f"Login failed: {str(e)}")
                return None
            finally:
                if browser:
                    browser.close()


class TokenStealer:
    @staticmethod
    def get_windows_clipboard():
        """Retrieves clipboard content using PowerShell."""
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", "Get-Clipboard"],
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except Exception:
            return ""

    @staticmethod
    def wait_for_token(timeout=60):
        """Waits for a JWT token to appear in the clipboard."""
        print("Waiting for token in clipboard (Manual Fallback)...")
        start_time = time.time()
        jwt_pattern = r"eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+"
        last_clip = ""

        while (time.time() - start_time) < timeout:
            current_clip = TokenStealer.get_windows_clipboard()
            if current_clip != last_clip:
                match = re.search(jwt_pattern, current_clip)
                if match:
                    token = match.group(0)
                    print("Token detected in clipboard.")
                    return token
                last_clip = current_clip
            time.sleep(1)
        return None


class PPCHarvester:
    def __init__(self, token):
        self.headers = config.get_headers(token)

    def xlsx_to_parquet(self, xlsx_path, start_iso, end_iso, suffix=""):
        """Converts Excel file to Parquet format using fastexcel directly."""
        try:
            # Use fastexcel directly to read to Arrow, then to Polars
            # This bypasses any engine string issues in pl.read_excel
            excel_reader = fastexcel.read_excel(xlsx_path)
            # Assuming data is in the first sheet
            arrow_table = excel_reader.load_sheet(0).to_arrow()
            df = pl.from_arrow(arrow_table)
            
            # Add range metadata columns
            df = df.with_columns([
                pl.lit(start_iso).alias("Date_Start"),
                pl.lit(end_iso).alias("Date_End"),
                # Keep Report_Date as End Date for backward compatibility with downstream logic
                pl.lit(end_iso).alias("Report_Date")
            ])
            
            filename = f"ppc_report_{start_iso}_{end_iso}{suffix}.parquet"
            parquet_path = os.path.join(config.SILVER_DATA_DIR, filename)
            df.write_parquet(parquet_path, compression="zstd")
            return parquet_path
        except Exception as e:
            print(f"Error converting {start_iso}_{end_iso} to Parquet: {e}")
            return None

    def fetch_data(self, start_date_str, end_date_str, step="day", dry_run=False, debug=False):
        """
        Iterates through date range based on granularity (step) and downloads reports.
        step: 'day', 'month', 'year', 'total'
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        current = start_date
        request_count = 0

        print(f"Starting fetch job. Range: {start_date_str} to {end_date_str}. Step: {step}")
        if dry_run:
            print("WARNING: DRY-RUN MODE ACTIVATED. No HTTP requests will be sent.")

        while current <= end_date:
            request_count += 1
            
            # Determine chunk end date based on step
            if step == "day":
                chunk_end = current
                suffix = "_daily"
            elif step == "month":
                # Get last day of the current month
                _, last_day = calendar.monthrange(current.year, current.month)
                chunk_end = current.replace(day=last_day)
                suffix = ""
            elif step == "year":
                chunk_end = current.replace(month=12, day=31)
                suffix = ""
            else: # total
                chunk_end = end_date
                suffix = ""

            # Clamp chunk_end to global end_date
            if chunk_end > end_date:
                chunk_end = end_date
            
            # Format dates for API
            c_start_iso = current.strftime("%Y-%m-%d")
            c_end_iso = chunk_end.strftime("%Y-%m-%d")
            
            print(f"[{request_count}] Processing range: {c_start_iso} to {c_end_iso}")

            params = {
                "page": 1,
                "period": "custom",
                "timeFrame": "custom",
                "fromDate": f"{c_start_iso}T00:00:00.000Z",
                "toDate": f"{c_end_iso}T23:59:59.000Z",
                "flag": 1,
                "fields": config.DEFAULT_FIELDS,
            }

            if debug:
                print(f"   [DEBUG] URL: {config.API_BASE_URL}")
                print(f"   [DEBUG] Params: {params}")

            if dry_run:
                print(f"   [DRY-RUN] Would fetch and save to: ppc_report_{c_start_iso}_{c_end_iso}{suffix}.parquet")
            else:
                try:
                    response = requests.get(
                        config.API_BASE_URL, headers=self.headers, params=params, timeout=60
                    )

                    if response.status_code == 200:
                        # Save raw with range in filename (Raw doesn't need suffix as much, but keep consistent)
                        xlsx_filename = f"ppc_report_{c_start_iso}_{c_end_iso}{suffix}.xlsx"
                        xlsx_path = os.path.join(config.RAW_DATA_DIR, xlsx_filename)
                        
                        with open(xlsx_path, "wb") as f:
                            f.write(response.content)
                        
                        self.xlsx_to_parquet(xlsx_path, c_start_iso, c_end_iso, suffix=suffix)
                        print(f"   Success: Saved {xlsx_filename}")
                    elif response.status_code == 401:
                        print("   Token expired during fetch!")
                        return False
                    else:
                        print(f"   Error: Status Code {response.status_code}")
                        if debug:
                            print(f"   [DEBUG] Response: {response.text}")

                except Exception as e:
                    print(f"   Exception: {str(e)}")

            # Advance current date
            if step == "total":
                break # Done after one shot
            
            current = chunk_end + timedelta(days=1)
            if not dry_run:
                time.sleep(1) # Be nice to the server

        return True


class DataProcessor:
    @staticmethod
    def clean_numeric(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        """Cleans numeric columns by removing symbols and casting to float."""
        if col_name in df.columns:
            return df.with_columns(
                pl.col(col_name)
                .cast(pl.String)
                .str.replace_all(r"[$,%]", "")
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
            )
        return df

    @staticmethod
    def process_and_merge(output_filename="Master_PPC_Data.parquet"):
        """Merges all Parquet files from Silver layer into a Master dataset."""
        import glob
        files = glob.glob(os.path.join(config.SILVER_DATA_DIR, "*.parquet"))

        if not files:
            print("No Parquet files found in Silver directory.")
            return None

        print(f"Merging {len(files)} Parquet files...")
        
        dfs = [pl.scan_parquet(f) for f in files]
        master_df = pl.concat(dfs).collect()

        numeric_cols = [
            "Unit sold (Actual)", "Ads Spend (Actual)", "Revenue (Actual)", 
            "Refund", "FBA Stock", "Phase"
        ]
        for col in numeric_cols:
            master_df = DataProcessor.clean_numeric(master_df, col)

        # Unique logic updated to include Date_Start to handle different ranges correctly
        # We keep the last record for the same SKU within the exact same Time Range
        if "Date_Start" in master_df.columns:
            subset_cols = ["SKU", "Date_Start", "Date_End"]
        else:
            subset_cols = ["SKU", "Report_Date"]

        master_df = master_df.unique(subset=subset_cols, keep="last").sort(
            subset_cols
        )

        output_path = os.path.join(config.OUTPUT_DIR, output_filename)
        master_df.write_parquet(output_path, compression="zstd")
        
        master_df.write_csv(output_path.replace(".parquet", ".csv"))
        
        print(f"Master File generated: {output_path} ({master_df.height} rows)")
        return master_df

    @staticmethod
    def compact_daily_to_monthly(year, month):
        """
        Maintenance Task: Merges all daily files of a specific month into one monthly file.
        Deletes daily files after successful merge to reduce clutter.
        """
        import glob
        
        # Pattern to match ONLY daily files for that month: ppc_report_2025-10-*_daily.parquet
        pattern = os.path.join(config.SILVER_DATA_DIR, f"ppc_report_{year}-{month:02d}-*_daily.parquet")
        daily_files = glob.glob(pattern)
        
        if not daily_files:
            print(f"No daily files found for {year}-{month:02d} to compact.")
            return

        print(f"Compacting {len(daily_files)} daily files for {year}-{month:02d}...")
        
        try:
            dfs = [pl.scan_parquet(f) for f in daily_files]
            combined_df = pl.concat(dfs).collect()
            
            # Save as Monthly (No suffix to indicate it's an aggregate)
            # Logic: Start of month to End of month
            _, last_day = calendar.monthrange(year, month)
            start_iso = f"{year}-{month:02d}-01"
            end_iso = f"{year}-{month:02d}-{last_day}"
            
            monthly_filename = f"ppc_report_{start_iso}_{end_iso}.parquet"
            monthly_path = os.path.join(config.SILVER_DATA_DIR, monthly_filename)
            
            combined_df.write_parquet(monthly_path, compression="zstd")
            print(f"✅ Created monthly archive: {monthly_filename}")
            
            # Delete daily files
            for f in daily_files:
                if os.path.abspath(f) != os.path.abspath(monthly_path):
                    os.remove(f)
            
            print(f"🗑️ Deleted {len(daily_files)} daily files.")
            
        except Exception as e:
            print(f"❌ Compaction failed: {e}")

    @staticmethod
    def compact_monthly_to_yearly(year):
        """
        Maintenance Task: Merges all monthly files of a specific year into one yearly file.
        Deletes monthly files after successful merge.
        """
        files_to_compact = []
        
        # Scan for expected monthly files (01 to 12)
        for month in range(1, 13):
            _, last_day = calendar.monthrange(year, month)
            # Match standard monthly filename format
            filename = f"ppc_report_{year}-{month:02d}-01_{year}-{month:02d}-{last_day}.parquet"
            filepath = os.path.join(config.SILVER_DATA_DIR, filename)
            
            if os.path.exists(filepath):
                files_to_compact.append(filepath)
        
        if not files_to_compact:
            print(f"No monthly files found for year {year} to compact.")
            return

        print(f"Compacting {len(files_to_compact)} monthly files for year {year}...")
        
        try:
            dfs = [pl.scan_parquet(f) for f in files_to_compact]
            combined_df = pl.concat(dfs).collect()
            
            # Save as Yearly
            yearly_filename = f"ppc_report_{year}-01-01_{year}-12-31.parquet"
            yearly_path = os.path.join(config.SILVER_DATA_DIR, yearly_filename)
            
            combined_df.write_parquet(yearly_path, compression="zstd")
            print(f"✅ Created yearly archive: {yearly_filename}")
            
            # Delete monthly files
            for f in files_to_compact:
                if os.path.abspath(f) != os.path.abspath(yearly_path):
                    os.remove(f)
            
            print(f"🗑️ Deleted {len(files_to_compact)} monthly files.")
            
        except Exception as e:
            print(f"❌ Yearly Compaction failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="PPC Scraper & Processor")
    parser.add_argument("--start", help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End Date (YYYY-MM-DD)")
    parser.add_argument("--step", choices=["day", "month", "year", "total"], default="day", help="Aggregation Granularity")
    parser.add_argument("--mode", choices=["full", "offline"], default="full", help="Operation Mode")
    parser.add_argument("--dry-run", action="store_true", help="Simulate run without making API requests")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    parser.add_argument("--compact", help="Run compaction. Format: 'YYYY-MM' (Daily->Month) or 'YYYY' (Month->Year)")
    
    args = parser.parse_args()

    # Maintenance Mode
    if args.compact:
        parts = args.compact.split("-")
        try:
            if len(parts) == 2:
                # Daily -> Monthly
                y, m = map(int, parts)
                DataProcessor.compact_daily_to_monthly(y, m)
            elif len(parts) == 1:
                # Monthly -> Yearly
                y = int(parts[0])
                DataProcessor.compact_monthly_to_yearly(y)
            else:
                print("Invalid format. Use YYYY-MM or YYYY.")
                return
            
            # Re-generate master after compaction
            DataProcessor.process_and_merge()
            print("Compaction & Master Re-build Complete.")
            return
        except ValueError:
            print("Invalid format for --compact.")
            return

    user = os.getenv("PPC_USER")
    password = os.getenv("PPC_PASS")
    token = None

    if args.mode == "full":
        # 1. Attempt Auto Login
        if args.dry_run:
             print("Info: DRY-RUN enabled. Skipping login authentication (using mock token).")
             token = "mock_token_dry_run"
        else:
            login_bot = AutoLogin(user, password)
            token = login_bot.get_token()

            # 2. Fallback to Clipboard
            if not token:
                token = TokenStealer.wait_for_token(timeout=30)

        if not token:
            print("Failed to retrieve token. Exiting.")
            return

        # 3. Fetch Data
        start_date = args.start or input("Start Date (YYYY-MM-DD): ").strip()
        end_date = args.end or input("End Date (YYYY-MM-DD): ").strip()
        step = args.step

        harvester = PPCHarvester(token)
        harvester.fetch_data(start_date, end_date, step=step, dry_run=args.dry_run, debug=args.debug)

    if args.dry_run:
        print("Info: DRY-RUN enabled. Skipping data processing.")
        return

    # 4. Process Data
    processor = DataProcessor()
    master_data = processor.process_and_merge()

    if master_data is not None:
        print("Operation Completed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Cancelled by user.")
    except Exception as e:
        print(f"System Error: {e}")
