import argparse
import calendar
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

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

                # Navigate to login page
                page.goto(self.login_url)
                
                # Wait for input fields
                page.wait_for_selector('input[name="username"], input[type="text"], input[type="email"]')
                
                # Identify username and password fields
                username_field = page.query_selector('input[name="username"]') or \
                                 page.query_selector('input[type="text"]') or \
                                 page.query_selector('input[type="email"]')
                
                password_field = page.query_selector('input[name="password"]') or \
                                 page.query_selector('input[type="password"]')

                if not username_field or not password_field:
                    print("Error: Could not locate login fields.")
                    return None

                # Input credentials
                username_field.fill(self.username)
                password_field.fill(self.password)
                
                # Submit form
                login_btn = page.query_selector('button[type="submit"]') or \
                            page.query_selector('button:has-text("Login")') or \
                            page.query_selector('.login-button')
                
                if login_btn:
                    login_btn.click()
                else:
                    page.keyboard.press("Enter")
                
                # Wait for navigation to dashboard
                print("Waiting for authentication...")
                page.wait_for_url("**/dashboard**", timeout=30000)
                print("Login successful.")

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
                
                browser.close()
                return token
            except Exception as e:
                print(f"Login failed: {str(e)}")
                return None


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

    def xlsx_to_parquet(self, xlsx_path, start_iso, end_iso):
        """Converts Excel file to Parquet format using Calamine engine with range metadata."""
        try:
            df = pl.read_excel(xlsx_path, engine="calamine")
            
            # Add range metadata columns
            df = df.with_columns([
                pl.lit(start_iso).alias("Date_Start"),
                pl.lit(end_iso).alias("Date_End"),
                # Keep Report_Date as End Date for backward compatibility with downstream logic
                pl.lit(end_iso).alias("Report_Date")
            ])
            
            parquet_path = os.path.join(
                config.SILVER_DATA_DIR, f"ppc_report_{start_iso}_{end_iso}.parquet"
            )
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
            elif step == "month":
                # Get last day of the current month
                _, last_day = calendar.monthrange(current.year, current.month)
                chunk_end = current.replace(day=last_day)
            elif step == "year":
                chunk_end = current.replace(month=12, day=31)
            else: # total
                chunk_end = end_date

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
                print(f"   [DRY-RUN] Would fetch and save to: ppc_report_{c_start_iso}_{c_end_iso}.xlsx")
            else:
                try:
                    response = requests.get(
                        config.API_BASE_URL, headers=self.headers, params=params, timeout=60
                    )

                    if response.status_code == 200:
                        # Save raw with range in filename
                        xlsx_filename = f"ppc_report_{c_start_iso}_{c_end_iso}.xlsx"
                        xlsx_path = os.path.join(config.RAW_DATA_DIR, xlsx_filename)
                        
                        with open(xlsx_path, "wb") as f:
                            f.write(response.content)
                        
                        self.xlsx_to_parquet(xlsx_path, c_start_iso, c_end_iso)
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


def main():
    parser = argparse.ArgumentParser(description="PPC Scraper & Processor")
    parser.add_argument("--start", help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End Date (YYYY-MM-DD)")
    parser.add_argument("--step", choices=["day", "month", "year", "total"], default="day", help="Aggregation Granularity")
    parser.add_argument("--mode", choices=["full", "offline"], default="full", help="Operation Mode")
    parser.add_argument("--dry-run", action="store_true", help="Simulate run without making API requests")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()

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
